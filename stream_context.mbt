///|
pub(all) struct ClipStreamOptions {
  context_bytes : Int
  context_messages : Int
  output_messages : UInt64
  output_payload_bytes : UInt64
  pending_bytes : Int
  writer : StreamOptions
  limits : Limits
}

///|
pub fn clip_stream_options() -> ClipStreamOptions {
  {
    context_bytes: 8 * 1024 * 1024,
    context_messages: 10000,
    output_messages: 10000000UL,
    output_payload_bytes: 107374182400UL,
    pending_bytes: 64 * 1024 * 1024,
    writer: stream_options(),
    limits: { ..default_limits(), max_records: 30000000, },
  }
}

///|
/// Bounded context plus one pulled message and the writer's chunk/summary state.
/// Sink publication is the caller's transaction. Failure leaves an invalid prefix.
pub fn stream_context_clip(
  source : QuerySource,
  sink : WriteSink,
  start : UInt64,
  end : UInt64,
  topics? : Array[String] = [],
  context? : Array[ContextRule] = [],
  options? : ClipStreamOptions = clip_stream_options(),
  auxiliary? : (StreamWriter) -> Unit raise McapError = fn(_) { () },
  provenance? : Json = Json::null(),
  cancelled? : () -> Bool = fn() { false },
) -> Json raise McapError {
  if start > end ||
    context.length() > 128 ||
    options.context_bytes < 0 ||
    options.context_messages < 0 ||
    options.pending_bytes < 1 ||
    options.output_messages < 1UL {
    raise Invalid("invalid streaming clip bounds or budgets")
  }
  let declarations = source.declaration_log()
  let names : Map[String, Bool] = Map([])
  let retained : Array[Message] = []
  let evidence : Array[Json] = []
  let mut context_bytes = 0
  for rule in context {
    if rule.topic == "" ||
      rule.depth < 1 ||
      rule.depth > 10000 ||
      names.contains(rule.topic) {
      raise Invalid("invalid or duplicate context rule")
    }
    names[rule.topic] = true
    let messages : Array[Message] = []
    if start > 0UL {
      let stream = source.stream(
        query={
          ..all_query(),
          topics: [rule.topic],
          start_time: time_before(start, rule.max_age),
          end_time: start - 1UL,
          descending: true,
          limit: rule.depth,
        },
        pending_bytes=options.pending_bytes,
      )
      defer stream.cancel()
      let mut previous : UInt64? = None
      while stream.next() is Some(message) {
        if cancelled() {
          raise Invalid("streaming clip cancelled")
        }
        if message.log_time >= start ||
          message.log_time < time_before(start, rule.max_age) ||
          (previous is Some(t) && message.log_time > t) ||
          messages.length() >= rule.depth ||
          retained.length() + messages.length() >= options.context_messages ||
          message.data.length() > options.context_bytes - context_bytes - 31 {
          raise Invalid("streaming context budget or source contract violated")
        }
        previous = Some(message.log_time)
        context_bytes += message.data.length() + 31
        messages.push(message)
      }
    }
    if messages.is_empty() && rule.required {
      raise Invalid("required context absent: " + rule.topic)
    }
    evidence.push({
      "topic": rule.topic,
      "retained": messages.length(),
      "missing": messages.is_empty(),
    })
    for i = messages.length() - 1; i >= 0; i = i - 1 {
      retained.push(messages[i])
    }
  }
  let ordered = build_query_index({ ..declarations, messages: retained, }).query(
    all_query(),
  )
  let writer = new_stream_writer(
    sink,
    profile=declarations.header.profile,
    options=options.writer,
    limits=options.limits,
  )
  defer writer.cancel()
  for schema in declarations.schemas {
    writer.add_schema(schema)
  }
  for channel in declarations.channels {
    writer.add_channel(channel)
  }
  let mut payload = 0UL
  let mut count = 0UL
  fn emit(message : Message) -> Unit raise McapError {
    if cancelled() {
      raise Invalid("streaming clip cancelled")
    }
    if count >= options.output_messages ||
      message.data.length().to_uint64() > options.output_payload_bytes - payload {
      raise Invalid("streaming clip output budget exceeded")
    }
    writer.append(message)
    payload += message.data.length().to_uint64()
    count += 1UL
  }
  for message in ordered {
    emit(message)
  }
  let stream = source.stream(
    query={ ..all_query(), topics, start_time: start, end_time: end, },
    pending_bytes=options.pending_bytes,
  )
  defer stream.cancel()
  let mut previous : UInt64? = None
  while stream.next() is Some(message) {
    if message.log_time < start ||
      message.log_time > end ||
      (previous is Some(t) && message.log_time < t) {
      raise Invalid("streaming clip order/interval violated")
    }
    previous = Some(message.log_time)
    emit(message)
  }
  if cancelled() {
    raise Invalid("streaming clip cancelled")
  }
  auxiliary(writer)
  let report : Json = {
    "start": start.to_string(),
    "end": end.to_string(),
    "messages": count.to_string(),
    "context_messages": retained.length(),
    "context_bytes": context_bytes,
    "payload_bytes": payload.to_string(),
    "context": evidence,
    "provenance": provenance,
    "auxiliary_policy": "caller supplied; absent callback excludes source auxiliary records",
  }
  writer.add_metadata({
    name: "moonmcap.stream-context",
    entries: [("manifest", report.stringify())],
  })
  let progress = writer.finish()
  {
    "clip": report,
    "output_bytes": progress.bytes_written.to_string(),
    "peak_chunk_bytes": progress.peak_buffered_bytes,
    "retained_summary_bytes": progress.retained_summary_bytes,
  }
}

///|
/// Copy all auxiliary records or explicit name allowlists. Message payloads are
/// scanned for container validation but never retained as a Log.
pub fn copy_clip_auxiliary(
  source : ReadSource,
  writer : StreamWriter,
  attachment_names? : Array[String] = [],
  metadata_names? : Array[String] = [],
  limits? : Limits = default_limits(),
  cancelled? : () -> Bool = fn() { false },
) -> UInt64 raise McapError {
  scan_source(
    source,
    source_visitor(
      attachment=fn(a) {
        if attachment_names.is_empty() || attachment_names.contains(a.name) {
          writer.add_attachment(a)
        }
      },
      metadata=fn(m) {
        if metadata_names.is_empty() || metadata_names.contains(m.name) {
          writer.add_metadata(m)
        }
      },
    ),
    limits~,
    cancelled~,
  )
}
