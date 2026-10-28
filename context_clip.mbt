///|
pub(all) struct ContextRule {
  topic : String
  depth : Int
  max_age : UInt64
  required : Bool
}

///|
pub fn context_rule(topic : String) -> ContextRule {
  { topic, depth: 1, max_age: 18446744073709551615UL, required: true, }
}

///|
pub(all) struct ContextClip {
  log : Log
  report : Json
}

///|
/// Export a bounded, independent message clip. Explicit context rules select
/// the last N messages per topic (not semantic state merging). Aux data is not
/// fetched by QuerySource; the manifest states this instead of implying retention.
pub fn extract_context_clip(
  source : QuerySource,
  start : UInt64,
  end : UInt64,
  topics? : Array[String] = [],
  context? : Array[ContextRule] = [],
  message_limit? : Int = 100000,
  byte_limit? : Int = 64 * 1024 * 1024,
  pending_bytes? : Int = 64 * 1024 * 1024,
  cancelled? : () -> Bool = fn() { false },
) -> ContextClip raise McapError {
  if start > end ||
    context.length() > 128 ||
    message_limit < 1 ||
    byte_limit < 1 ||
    pending_bytes < 1 {
    raise Invalid("invalid context clip interval or budgets")
  }
  let template = source.declaration_log()
  let messages : Array[Message] = []
  let mut bytes = 0
  let names : Map[String, Bool] = {}
  let evidence : Array[Json] = []
  fn retain(message : Message) -> Unit raise McapError {
    if messages.length() >= message_limit ||
      message.data.length() > byte_limit - bytes - 31 {
      raise Invalid("clip message count or payload budget exceeded")
    }
    messages.push(message)
    bytes += message.data.length() + 31
  }
  for rule in context {
    if rule.topic == "" ||
      rule.depth < 1 ||
      rule.depth > 10000 ||
      names.contains(rule.topic) {
      raise Invalid("invalid or duplicate context rule")
    }
    names[rule.topic] = true
    let retained : Array[Message] = []
    let mut retained_bytes = 0
    if start > 0UL {
      let stream = (source.open)(
        {
          ..all_query(),
          topics: [rule.topic],
          start_time: time_before(start, rule.max_age),
          end_time: start - 1UL,
          descending: true,
          limit: rule.depth,
        },
        pending_bytes,
      )
      defer (stream.stop)()
      while (stream.next_message)() is Some(message) {
        if cancelled() {
          raise Invalid("context extraction cancelled")
        }
        if retained.length() >= rule.depth ||
          message.log_time >= start ||
          message.log_time < time_before(start, rule.max_age) ||
          message.data.length() > byte_limit - bytes - retained_bytes - 31 {
          raise Invalid("context budget or provider interval violated")
        }
        retained.push(message)
        retained_bytes += message.data.length() + 31
      }
    }
    if retained.is_empty() && rule.required {
      raise Invalid("required context absent: " + rule.topic)
    }
    // Restore source-stable ascending order, including equal-time messages.
    for i = retained.length() - 1; i >= 0; i = i - 1 {
      retain(retained[i])
    }
    evidence.push({
      "topic": rule.topic,
      "requested_depth": rule.depth,
      "retained": retained.length(),
      "missing": retained.is_empty(),
      "messages": retained.map(fn(m) { event_context_message_json(m) }),
    })
  }
  let context_count = messages.length()
  let stream = (source.open)(
    { ..all_query(), topics, start_time: start, end_time: end, },
    pending_bytes,
  )
  defer (stream.stop)()
  let mut previous : UInt64? = None
  while (stream.next_message)() is Some(message) {
    if cancelled() {
      raise Invalid("clip extraction cancelled")
    }
    if message.log_time < start ||
      message.log_time > end ||
      (previous is Some(t) && message.log_time < t) {
      raise Invalid("clip provider interval/order violated")
    }
    previous = Some(message.log_time)
    retain(message)
  }
  let report : Json = {
    "start": start.to_string(),
    "end": end.to_string(),
    "window_messages": messages.length() - context_count,
    "context_messages": context_count,
    "payload_bytes": bytes,
    "context": evidence,
    "auxiliary_policy": "message clip; source attachments and metadata excluded",
  }
  let log = {
    ..template,
    messages,
    metadata: [
      { name: "moonmcap.context", entries: [("manifest", report.stringify())], },
    ],
  }
  // slice_log prunes declarations and stably orders all retained messages.
  { log: slice_log(log, all_query()), report, }
}

///|
fn event_context_message_json(message : Message) -> Json {
  {
    "channel_id": message.channel_id,
    "sequence": message.sequence.to_string(),
    "log_time": message.log_time.to_string(),
  }
}
