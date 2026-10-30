///|
struct SourceTables {
  log : Log
  schemas : Map[Int32, Int32]
  channels : Map[Int32, Int32]
  counts : Map[Int32, UInt64]
  attachments : Map[UInt64, Bytes]
  metadata : Map[UInt64, Bytes]
  mut retained : Int32
  mut messages : UInt64
  mut chunks : UInt32
  mut first : UInt64
  mut last : UInt64
}

///|
fn SortSession::reserve_declaration(
  self : SortSession,
  size : Int32,
) -> Unit raise McapError {
  if size < 0 || size > self.options.declaration_bytes - self.declared {
    raise Invalid("global declaration budget exceeded")
  }
  self.declared += size
}

///|
fn SourceTables::reserve(
  self : SourceTables,
  size : Int32,
  maximum : Int32,
) -> Unit raise McapError {
  if size < 0 || size > maximum - self.retained {
    raise Invalid("source declaration/index metadata budget exceeded")
  }
  self.retained += size
}

///|
fn SortSession::map_schema(
  self : SortSession,
  tables : SourceTables,
  global : Log,
  writer : StreamWriter,
  schema : Schema,
) -> Unit raise McapError {
  add_schema(tables.log, schema)
  if tables.schemas.contains(schema.id) {
    return
  }
  tables.reserve(encode_schema(schema).length(), self.options.declaration_bytes)
  if self.observer is Some(visitor) {
    (visitor.schema)(schema)
  }
  let existing = if self.preserve_ids {
    None
  } else {
    global.schemas
    .iter()
    .find_first(fn(s) {
      s.name == schema.name &&
      s.encoding == schema.encoding &&
      s.data == schema.data
    })
  }
  match existing {
    Some(value) => tables.schemas[schema.id] = value.id
    None => {
      let id = if self.preserve_ids {
        schema.id
      } else {
        next_id(global.schemas.length())
      }
      let mapped = { ..schema, id, }
      self.reserve_declaration(encode_schema(mapped).length())
      writer.add_schema(mapped)
      global.schemas.push(mapped)
      tables.schemas[schema.id] = id
    }
  }
}

///|
fn SortSession::map_channel(
  self : SortSession,
  tables : SourceTables,
  global : Log,
  writer : StreamWriter,
  channel : Channel,
) -> Unit raise McapError {
  add_channel(tables.log, channel)
  if tables.channels.contains(channel.id) {
    return
  }
  tables.reserve(
    encode_channel(channel).length(),
    self.options.declaration_bytes,
  )
  if self.observer is Some(visitor) {
    (visitor.channel)(channel)
  }
  let schema_id = tables.schemas[channel.schema_id]
  let existing = if self.preserve_ids {
    None
  } else {
    global.channels
    .iter()
    .find_first(fn(c) {
      c.topic == channel.topic &&
      c.schema_id == schema_id &&
      c.message_encoding == channel.message_encoding &&
      c.metadata == channel.metadata
    })
  }
  match existing {
    Some(value) => tables.channels[channel.id] = value.id
    None => {
      let id = if self.preserve_ids {
        channel.id
      } else {
        next_id(global.channels.length())
      }
      let mapped = { ..channel, id, schema_id, }
      self.reserve_declaration(encode_channel(mapped).length())
      writer.add_channel(mapped)
      global.channels.push(mapped)
      tables.channels[channel.id] = id
    }
  }
}

///|
fn SortSession::ingest_message(
  self : SortSession,
  tables : SourceTables,
  body : Bytes,
  runs : Array[Int32],
  location : MessageLocation,
) -> Unit raise McapError {
  let message = decode_message(body, limits=self.options.limits)
  if !tables.channels.contains(message.channel_id) {
    raise Invalid("external input Message has no prior Channel")
  }
  if self.ordinal >= self.options.limits.max_records.to_uint64() {
    raise Invalid("external total message count budget exceeded")
  }
  if self.observer is Some(visitor) {
    (visitor.message)(message, location)
    self.ordinal += 1UL
  } else {
    self.add(
      { ..message, channel_id: tables.channels[message.channel_id], },
      runs,
    )
  }
  tables.counts[message.channel_id] = tables.counts
    .get(message.channel_id)
    .unwrap_or(0UL) +
    1UL
  if tables.messages == 0UL || message.log_time < tables.first {
    tables.first = message.log_time
  }
  if tables.messages == 0UL || message.log_time > tables.last {
    tables.last = message.log_time
  }
  tables.messages += 1UL
}

///|
fn SortSession::check_input_summary(
  self : SortSession,
  source : ReadSource,
  reader : RangeReader,
  tables : SourceTables,
  records_before_summary : Int32,
) -> Unit raise McapError {
  let start = reader.data_end + 13UL
  let size = bounded_length(
    source.size - 37UL - start,
    self.options.declaration_bytes,
    "input summary",
  )
  let records = decode_records(
    source.read_exact(start, size),
    limits=self.options.limits,
  )
  if records.length() >
    self.options.limits.max_records - records_before_summary - 1 {
    raise Invalid("external input total record budget exceeded")
  }
  let attached : Map[UInt64, Bool] = Map([])
  let metadata : Map[UInt64, Bool] = Map([])
  let channels : Map[Int32, Bool] = Map([])
  for record in records {
    match record.opcode {
      0x03 =>
        if !tables.log.schemas.contains(
            decode_schema(record.body, limits=self.options.limits),
          ) {
          raise Invalid("external input summary Schema is not a data copy")
        }
      0x04 => {
        let channel = decode_channel(record.body, limits=self.options.limits)
        if !tables.log.channels.contains(channel) {
          raise Invalid("external input summary Channel is not a data copy")
        }
        channels[channel.id] = true
      }
      0x0a => {
        let index = decode_attachment_index(
          record.body,
          limits=self.options.limits,
        )
        if attached.contains(index.offset) ||
          tables.attachments.get(index.offset) !=
          Some(encode_attachment_index(index)) {
          raise Invalid("external input AttachmentIndex mismatch")
        }
        attached[index.offset] = true
      }
      0x0d => {
        let index = decode_metadata_index(
          record.body,
          limits=self.options.limits,
        )
        if metadata.contains(index.offset) ||
          tables.metadata.get(index.offset) !=
          Some(encode_metadata_index(index)) {
          raise Invalid("external input MetadataIndex mismatch")
        }
        metadata[index.offset] = true
      }
      0x0b => {
        let stats = decode_file_statistics(
          record.body,
          limits=self.options.limits,
        )
        if stats.message_count != tables.messages ||
          stats.chunk_count != tables.chunks ||
          stats.schema_count != tables.log.schemas.length() ||
          stats.channel_count.to_uint64() !=
          tables.log.channels.length().to_uint64() ||
          stats.attachment_count.to_uint64() !=
          tables.attachments.length().to_uint64() ||
          stats.metadata_count.to_uint64() !=
          tables.metadata.length().to_uint64() ||
          stats.start_time != tables.first ||
          stats.end_time != tables.last {
          raise Invalid("external input Statistics mismatch")
        }
        if !stats.channel_message_counts.is_empty() {
          if tables.log.channels.any(fn(c) { !channels.contains(c.id) }) {
            raise Invalid("Statistics requires prior summary channels")
          }
          for (id, count) in stats.channel_message_counts {
            if !tables.channels.contains(id) ||
              tables.counts.get(id).unwrap_or(0UL) != count {
              raise Invalid("external input channel statistics mismatch")
            }
          }
          for (id, count) in tables.counts {
            if !stats.channel_message_counts.contains((id, count)) {
              raise Invalid("external input missing channel count")
            }
          }
        }
      }
      _ => ()
    }
  }
  if !attached.is_empty() && attached.length() != tables.attachments.length() {
    raise Invalid("external input incomplete attachment indexes")
  }
  if !metadata.is_empty() && metadata.length() != tables.metadata.length() {
    raise Invalid("external input incomplete metadata indexes")
  }
}

///|
fn SortSession::ingest(
  self : SortSession,
  source : ReadSource,
  global : Log,
  writer : StreamWriter,
  runs : Array[Int32],
) -> Unit raise McapError {
  let limits = self.options.limits
  let reader = open_reader(
    source,
    limits~,
    cache_bytes=0,
    summary_bytes=self.options.declaration_bytes,
    fallback_bytes=0,
  )
  if reader.declarations.header.profile != global.header.profile {
    raise Invalid("external merge profile mismatch")
  }
  let tables : SourceTables = {
    log: { ..empty_log(), header: reader.declarations.header, },
    schemas: Map([(0, 0)]),
    channels: Map([]),
    counts: Map([]),
    attachments: Map([]),
    metadata: Map([]),
    retained: 0,
    messages: 0UL,
    chunks: 0U,
    first: 0UL,
    last: 0UL,
  }
  let indexes : Map[UInt64, ChunkIndex] = Map([])
  for index in reader.indexes {
    indexes[index.chunk_start] = index
  }
  let mut crc = crc32(source.read_exact(0UL, 8))
  let mut position = 8UL
  let mut count = 0
  while position <= reader.data_end {
    self.check_cancel()
    if count >= limits.max_records {
      raise Invalid("external input record budget exceeded")
    }
    self.trace_event(RecordStart(position))
    let record = source.record(position, reader.data_end + 13UL, limits)
    self.trace_event(
      RecordRead(position, record.opcode, record.body.length() + 9),
    )
    if position == 8UL && record.opcode != 0x01 {
      raise Invalid("external input first record is not Header")
    }
    match record.opcode {
      0x01 => {
        if position != 8UL {
          raise Invalid("external input duplicate Header")
        }
        if decode_header(record.body, limits~) != reader.declarations.header {
          raise Invalid("external input Header changed while merging")
        }
      }
      0x03 =>
        self.map_schema(
          tables,
          global,
          writer,
          decode_schema(record.body, limits~),
        )
      0x04 =>
        self.map_channel(
          tables,
          global,
          writer,
          decode_channel(record.body, limits~),
        )
      0x05 => {
        if !indexes.is_empty() {
          raise Invalid("indexed external input has unchunked Message")
        }
        self.ingest_message(tables, record.body, runs, {
          offset: position,
          span: record.body.length() + 9,
          inner: None,
        })
      }
      0x06 => {
        let chunk = if indexes.is_empty() {
          decode_chunk(record.body, limits~)
        } else {
          match indexes.get(position) {
            Some(index) => reader.load_chunk(index)
            None => raise Invalid("external input missing ChunkIndex")
          }
        }
        if chunk.records.length() > limits.max_records - count - 1 {
          raise Invalid("external nested record budget exceeded")
        }
        count += chunk.records.length()
        for nested in chunk.records {
          match nested.opcode {
            0x03 =>
              self.map_schema(
                tables,
                global,
                writer,
                decode_schema(nested.body, limits~),
              )
            0x04 =>
              self.map_channel(
                tables,
                global,
                writer,
                decode_channel(nested.body, limits~),
              )
            0x05 =>
              self.ingest_message(tables, nested.body, runs, {
                offset: position,
                span: record.body.length() + 9,
                inner: Some(nested.offset),
              })
            _ => ()
          }
        }
        tables.chunks += 1U
      }
      0x07 => ignore(decode_message_index(record.body, limits~))
      0x09 => {
        let value = decode_attachment(record.body, limits~)
        let index = encode_attachment_index({
          offset: position,
          length: (record.body.length() + 9).to_uint64(),
          log_time: value.log_time,
          create_time: value.create_time,
          data_size: value.data.length().to_uint64(),
          name: value.name,
          media_type: value.media_type,
        })
        tables.reserve(index.length(), self.options.declaration_bytes)
        tables.attachments[position] = index
        if self.observer is Some(visitor) {
          (visitor.attachment)(value)
        }
        writer.add_attachment(value)
      }
      0x0c => {
        let value = decode_metadata(record.body, limits~)
        let index = encode_metadata_index({
          offset: position,
          length: (record.body.length() + 9).to_uint64(),
          name: value.name,
        })
        tables.reserve(index.length(), self.options.declaration_bytes)
        tables.metadata[position] = index
        if self.observer is Some(visitor) {
          (visitor.metadata)(value)
        }
        writer.add_metadata(value)
      }
      0x0f => {
        if position != reader.data_end {
          raise Invalid("external input premature DataEnd")
        }
        let value = cursor(record.body, limits)
        let expected = value.u32()
        value.finish()
        self.trace_event(DataChecksum(position, expected, crc))
        if expected != 0U && expected != crc {
          raise Invalid("external input data CRC mismatch")
        }
        if !indexes.is_empty() &&
          tables.chunks.to_uint64() != indexes.length().to_uint64() {
          raise Invalid("external input orphan ChunkIndex")
        }
        self.trace_event(DataValidated(position, expected != 0U))
        self.trace_event(SummaryStart(position + 13UL))
        self.check_input_summary(source, reader, tables, count + 1)
        return
      }
      0x00 | 0x02 | 0x08 | 0x0a | 0x0b | 0x0d | 0x0e =>
        raise Invalid("external input summary record inside data")
      _ => ()
    }
    self.trace_event(
      RecordAccepted(position, record.opcode, record.body.length() + 9),
    )
    crc = crc32_extend(crc, frame(record.opcode, record.body))
    position += (record.body.length() + 9).to_uint64()
    count += 1
  }
  raise Invalid("external input missing DataEnd")
}

///|
/// Stable external sorting of full MCAP inputs; temporary storage is caller-selected.
pub fn external_merge(
  sources : Array[ReadSource],
  storage : SortStorage,
  output : WriteSink,
  options~ : ExternalOptions = external_options(),
  cancelled~ : () -> Bool = fn() { false },
  preserve_ids~ : Bool = false,
) -> ExternalReport raise McapError {
  let session : SortSession = {
    observer: None,
    trace: None,
    preserve_ids,
    storage,
    options,
    live: Map([]),
    cancelled,
    buffer: [],
    buffered: 0,
    ordinal: 0UL,
    next_id: 0,
    spool: 0UL,
    peak_spool: 0UL,
    peak_run: 0,
    peak_merge: 0,
    passes: 0,
    declared: 0,
  }
  let result = try {
    if sources.is_empty() ||
      (preserve_ids && sources.length() != 1) ||
      options.run_bytes < 43 ||
      options.merge_bytes < 43 ||
      options.fan_in < 2 ||
      options.fan_in > 64 ||
      options.run_limit <= 0 ||
      options.declaration_bytes < 0 {
      raise Invalid("invalid external merge sources or budgets")
    }
    validate_log(empty_log(), limits=options.limits)
    session.check_cancel()
    let first = open_reader(
      sources[0],
      limits=options.limits,
      cache_bytes=0,
      summary_bytes=options.declaration_bytes,
      fallback_bytes=0,
    )
    let global = { ..empty_log(), header: first.declarations.header, }
    let writer = new_stream_writer(
      output,
      profile=global.header.profile,
      options=options.output,
      limits=options.limits,
    )
    let runs = []
    for source in sources {
      session.ingest(source, global, writer, runs)
    }
    if session.flush_run() is Some(id) {
      runs.push(id)
    }
    session.sort_to(runs, writer)
    let progress = writer.finish()
    Ok({
      input_files: sources.length(),
      messages: session.ordinal,
      runs_created: session.next_id,
      merge_passes: session.passes,
      peak_run_bytes: session.peak_run,
      peak_merge_bytes: session.peak_merge,
      peak_spool_bytes: session.peak_spool,
      output_bytes: progress.bytes_written,
    })
  } catch {
    error => Err(error)
  }
  let cleanup = []
  for id in session.live.keys().to_array() {
    session.remove(id) catch {
      Invalid(reason) => cleanup.push(reason)
    }
  }
  (storage.finish)() catch {
    Invalid(reason) => cleanup.push(reason)
  }
  match result {
    Err(Invalid(reason)) => {
      if !cleanup.is_empty() {
        raise Invalid("\{reason}; cleanup failed: \{cleanup[0]}")
      }
      raise Invalid(reason)
    }
    Ok(report) => {
      if !cleanup.is_empty() {
        raise Invalid("external merge cleanup failed: \{cleanup[0]}")
      }
      report
    }
  }
}
