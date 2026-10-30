///|
pub(all) struct TransformReport {
  input_messages : UInt64
  sampled_messages : Int32
  output_messages : Int32
  output_bytes : UInt64
  peak_temporary_bytes : UInt64
}

///|
fn transform_clock(
  time : UInt64,
  origin : UInt64,
  target : UInt64,
) -> UInt64 raise McapError {
  if target >= origin {
    let delta = target - origin
    if time > 18446744073709551615UL - delta {
      raise Invalid("transform timestamp overflow")
    }
    time + delta
  } else {
    let delta = origin - target
    if time < delta {
      raise Invalid("transform timestamp underflow")
    }
    time - delta
  }
}

///|
fn sampled_message(
  message : Message,
  options : TransformOptions,
  buckets : Map[Int32, UInt64],
) -> Bool {
  if options.sample_period == 0UL {
    return true
  }
  if message.log_time < options.sample_origin {
    return false
  }
  let bucket = (message.log_time - options.sample_origin) /
    options.sample_period
  if buckets.get(message.channel_id) == Some(bucket) {
    return false
  }
  buckets[message.channel_id] = bucket
  true
}

///|
/// Multi-pass bounded transformation. Sorting, selected messages and merge runs
/// share the same physical temporary-space budget. No historical Message array.
pub fn stream_transform(
  source : ReadSource,
  storage : SortStorage,
  output : WriteSink,
  options : TransformOptions,
  external~ : ExternalOptions = external_options(),
  content? : JsonFilter,
  cancelled~ : () -> Bool = fn() { false },
) -> TransformReport raise McapError {
  let work : TemporaryWorkspace = {
    storage,
    maximum: external.spool_bytes,
    live: Map([]),
    bytes: 0UL,
    peak: 0UL,
  }
  let result = try {
    let checked = { ..options, align_start: None, }
    // Existing validation checks mappings against source declarations after normalization.
    if options.query.start_time > options.query.end_time ||
      options.query.offset < 0 ||
      options.query.limit < 0 {
      raise Invalid("invalid streaming transform query")
    }
    let normalized_sink = work.create(-1)
    let nested = sort_storage(
      fn(id) { work.create(id) },
      fn(id) { (storage.open)(id) },
      fn(id) { work.remove(id) },
    )
    let sorted = external_merge(
      [source],
      nested,
      normalized_sink,
      options=external,
      cancelled~,
      preserve_ids=true,
    )
    let normalized = (storage.open)(-1)
    let reader = open_reader(
      normalized,
      limits=external.limits,
      cache_bytes=0,
      summary_bytes=external.declaration_bytes,
      fallback_bytes=0,
    )
    let declarations = reader.declarations
    ignore(transform_log(declarations, checked))
    let query = {
      ..options.query,
      descending: false,
      offset: 0,
      limit: external.limits.max_records,
    }
    let buckets : Map[Int32, UInt64] = Map([])
    let count = [0]
    if !reader.indexes.is_empty() {
      let cursor = reader.cursor(query)
      while cursor.next() is Some(message) {
        if cancelled() {
          cursor.cancel()
          raise Invalid("stream transform cancelled")
        }
        if (match content {
            None => true
            Some(filter) =>
              matches_json_message(message, declarations.channels, filter)
          }) &&
          sampled_message(message, options, buckets) {
          count[0] += 1
        }
      }
    }
    let skipped = options.query.offset.min(count[0])
    let selected = options.query.limit.min(count[0] - skipped)
    if selected == 0 && options.align_start is Some(_) {
      raise Invalid("cannot align an empty message selection")
    }
    let begin = if options.query.descending {
      count[0] - skipped - selected
    } else {
      skipped
    }
    let ending = begin + selected
    let staged = new_stream_writer(
      work.create(-2),
      profile=declarations.header.profile,
      limits=external.limits,
    )
    let used : Map[Int32, Bool] = Map([])
    let origin = [0UL]
    buckets.clear()
    let ordinal = [0]
    if selected > 0 {
      let cursor = reader.cursor(query)
      while cursor.next() is Some(message) {
        if cancelled() {
          cursor.cancel()
          raise Invalid("stream transform cancelled")
        }
        if !(match content {
            None => true
            Some(filter) =>
              matches_json_message(message, declarations.channels, filter)
          }) ||
          !sampled_message(message, options, buckets) {
          continue
        }
        let index = ordinal[0]
        ordinal[0] += 1
        if index < begin {
          continue
        }
        if index >= ending {
          break
        }
        if index == begin {
          origin[0] = message.log_time
        }
        if !used.contains(message.channel_id) {
          let channel = declarations.channels
            .iter()
            .find_first(fn(c) { c.id == message.channel_id })
            .unwrap()
          if channel.schema_id != 0 {
            let schema = declarations.schemas
              .iter()
              .find_first(fn(s) { s.id == channel.schema_id })
              .unwrap()
            // StreamWriter accepts identical repeated schema declarations.
            staged.add_schema(schema)
          }
          let rename = options.topic_map
            .iter()
            .find_first(fn(pair) { pair.0 == channel.topic })
          staged.add_channel({
            ..channel,
            topic: rename.map(fn(pair) { pair.1 }).unwrap_or(channel.topic),
          })
          used[channel.id] = true
        }
        let mapped = match options.align_start {
          None => message
          Some(target) =>
            {
              ..message,
              log_time: transform_clock(message.log_time, origin[0], target),
              publish_time: transform_clock(
                message.publish_time,
                origin[0],
                target,
              ),
            }
        }
        staged.append(mapped)
      }
    }
    ignore(staged.finish())
    let prepared = open_reader(
      (storage.open)(-2),
      limits=external.limits,
      cache_bytes=0,
      fallback_bytes=0,
    )
    let writer = new_stream_writer(
      output,
      profile=declarations.header.profile,
      options=external.output,
      limits=external.limits,
    )
    for schema in prepared.declarations.schemas {
      writer.add_schema(schema)
    }
    for channel in prepared.declarations.channels {
      writer.add_channel(channel)
    }
    if selected > 0 {
      let cursor = prepared.cursor({
        ..all_query(),
        descending: options.query.descending,
        limit: selected,
      })
      while cursor.next() is Some(message) {
        if cancelled() {
          cursor.cancel()
          raise Invalid("stream transform cancelled")
        }
        writer.append(message)
      }
    }
    ignore(
      scan_source(
        normalized,
        source_visitor(
          attachment=fn(value) {
            if cancelled() {
              raise Invalid("stream transform cancelled")
            }
            if (
                !options.attachment_names.is_empty() &&
                !options.attachment_names.contains(value.name)
              ) ||
              (
                !options.attachment_media_types.is_empty() &&
                !options.attachment_media_types.contains(value.media_type)
              ) ||
              value.log_time < options.attachment_start ||
              value.log_time > options.attachment_end {
              return
            }
            let mapped = match options.align_start {
              None => value
              Some(target) =>
                {
                  ..value,
                  log_time: transform_clock(value.log_time, origin[0], target),
                  create_time: transform_clock(
                    value.create_time,
                    origin[0],
                    target,
                  ),
                }
            }
            writer.add_attachment(mapped)
          },
          metadata=fn(value) {
            if (
                options.metadata_names.is_empty() ||
                options.metadata_names.contains(value.name)
              ) &&
              options.metadata_entries.all(fn(pair) {
                value.entries.contains(pair)
              }) {
              writer.add_metadata(value)
            }
          },
        ),
        limits=external.limits,
        declaration_bytes=external.declaration_bytes,
        cancelled~,
      ),
    )
    let progress = writer.finish()
    Ok({
      input_messages: sorted.messages,
      sampled_messages: count[0],
      output_messages: selected,
      output_bytes: progress.bytes_written,
      peak_temporary_bytes: work.peak,
    })
  } catch {
    error => Err(error)
  }
  work.finish() catch {
    Invalid(cleanup) =>
      match result {
        Err(Invalid(reason)) => raise Invalid("\{reason}; \{cleanup}")
        Ok(_) => raise Invalid(cleanup)
      }
  }
  match result {
    Ok(report) => report
    Err(error) => raise error
  }
}

///|
pub fn transform_report_json(report : TransformReport) -> Json {
  {
    "input_messages": report.input_messages.to_string(),
    "sampled_messages": report.sampled_messages,
    "output_messages": report.output_messages,
    "output_bytes": report.output_bytes.to_string(),
    "peak_temporary_bytes": report.peak_temporary_bytes.to_string(),
  }
}

///|
pub fn FileCursor::write_ndjson(
  self : FileCursor,
  sink : WriteSink,
) -> Int32 raise McapError {
  let mut count = 0
  while self.next() is Some(message) {
    let text = messages_json([message], self.reader.declarations.channels).stringify()
    // Remove the one-element array wrapper, retaining one JSON object per line.
    sink.write(@utf8.encode((text[1:text.length() - 1].to_string() + "\n")[:]))
    count += 1
  }
  count
}
