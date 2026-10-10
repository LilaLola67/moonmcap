///|
/// Select messages while pruning unreachable declarations and retaining metadata.
pub fn slice_log(log : Log, query : Query) -> Log raise McapError {
  let messages = build_query_index(log).query(query)
  let needed_channels : Map[Int, Bool] = {}
  for message in messages {
    needed_channels[message.channel_id] = true
  }
  let channels = log.channels.filter(fn(c) { needed_channels.contains(c.id) })
  let needed_schemas : Map[Int, Bool] = {}
  for channel in channels {
    if channel.schema_id != 0 {
      needed_schemas[channel.schema_id] = true
    }
  }
  let schemas = log.schemas.filter(fn(s) { needed_schemas.contains(s.id) })
  copy_log({ ..log, schemas, channels, messages, })
}

///|
fn next_id(current : Int) -> Int raise McapError {
  if current >= 65535 {
    raise Invalid("identifier space exhausted during merge")
  }
  current + 1
}

///|
/// Deduplicate declarations by content, not source-local identifiers.
pub fn merge_logs(
  logs : Array[Log],
  chronological? : Bool = true,
) -> Log raise McapError {
  if logs.is_empty() {
    return empty_log()
  }
  let header : Header = { ..logs[0].header, library: "moonmcap/0.1.0 merge", }
  let merged : Log = {
    header,
    schemas: [],
    channels: [],
    messages: [],
    metadata: [],
    attachments: [],
  }
  let mut schema_id = 0
  let mut channel_id = 0
  for log in logs {
    validate_log(log)
    if log.header.profile != header.profile {
      raise Invalid("cannot merge incompatible profiles")
    }
    let schema_remap : Map[Int, Int] = { 0: 0 }
    for schema in log.schemas {
      match
        merged.schemas
        .iter()
        .find_first(fn(s) {
          s.name == schema.name &&
          s.encoding == schema.encoding &&
          s.data == schema.data
        }) {
        Some(existing) => schema_remap[schema.id] = existing.id
        None => {
          schema_id = next_id(schema_id)
          merged.schemas.push({
            ..schema,
            id: schema_id,
            data: schema.data.copy(),
          })
          schema_remap[schema.id] = schema_id
        }
      }
    }
    let channel_remap : Map[Int, Int] = {}
    for channel in log.channels {
      let remapped_schema = schema_remap[channel.schema_id]
      match
        merged.channels
        .iter()
        .find_first(fn(c) {
          c.schema_id == remapped_schema &&
          c.topic == channel.topic &&
          c.message_encoding == channel.message_encoding &&
          c.metadata == channel.metadata
        }) {
        Some(existing) => channel_remap[channel.id] = existing.id
        None => {
          channel_id = next_id(channel_id)
          merged.channels.push({
            ..channel,
            id: channel_id,
            schema_id: remapped_schema,
            metadata: channel.metadata.copy(),
          })
          channel_remap[channel.id] = channel_id
        }
      }
    }
    for message in log.messages {
      merged.messages.push({
        ..message,
        channel_id: channel_remap[message.channel_id],
        data: message.data.copy(),
      })
    }
    for metadata in log.metadata {
      merged.metadata.push({ ..metadata, entries: metadata.entries.copy(), })
    }
    for attachment in log.attachments {
      merged.attachments.push({ ..attachment, data: attachment.data.copy() })
    }
  }
  if chronological {
    let positions = []
    for i = 0; i < merged.messages.length(); i = i + 1 {
      positions.push(i)
    }
    sort_positions(merged, positions)
    let ordered = positions.map(fn(i) { merged.messages[i] })
    merged.messages.clear()
    for message in ordered {
      merged.messages.push(message)
    }
  }
  validate_log(merged)
  merged
}

///|
/// Shift both clocks by a signed magnitude without signed-integer narrowing.
pub fn shift_time(
  log : Log,
  magnitude : UInt64,
  backwards? : Bool = false,
) -> Log raise McapError {
  validate_log(log)
  let result = copy_log(log)
  for i = 0; i < result.messages.length(); i = i + 1 {
    let message = result.messages[i]
    let log_time = shift_clock(message.log_time, magnitude, backwards)
    let publish_time = shift_clock(message.publish_time, magnitude, backwards)
    result.messages[i] = { ..message, log_time, publish_time, }
  }
  for i = 0; i < result.attachments.length(); i = i + 1 {
    let attachment = result.attachments[i]
    result.attachments[i] = { ..attachment,
      log_time: shift_clock(attachment.log_time, magnitude, backwards),
      create_time: shift_clock(attachment.create_time, magnitude, backwards) }
  }
  result
}

///|
fn shift_clock(
  value : UInt64,
  magnitude : UInt64,
  backwards : Bool,
) -> UInt64 raise McapError {
  if backwards {
    if value < magnitude {
      raise Invalid("timestamp underflow")
    }
    value - magnitude
  } else {
    if value > 18446744073709551615UL - magnitude {
      raise Invalid("timestamp overflow")
    }
    value + magnitude
  }
}
