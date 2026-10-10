///|
pub(all) struct QueryDiagnostics {
  total_chunks : Int
  candidate_chunks : Int
  matching_messages : Int
  used_file_indexes : Bool
}

///|
pub(all) struct QueryResult {
  messages : Array[Message]
  diagnostics : QueryDiagnostics
}

///|
/// Query already-loaded bytes using MCAP chunk ranges and channel index maps.
/// Structural validation still scans all records; this is not disk-lazy I/O.
pub fn query_file(
  bytes : Bytes,
  query : Query,
  limits? : Limits = default_limits(),
) -> QueryResult raise McapError {
  if query.start_time > query.end_time || query.offset < 0 || query.limit < 0 {
    raise Invalid("invalid query range or pagination")
  }
  let indexes = validate_file_indexes(bytes, limits~)
  if indexes.is_empty() {
    let log = read_log(bytes, limits~)
    let all = { ..query, offset: 0, limit: limits.max_records, }
    let matching_messages = build_query_index(log).query(all).length()
    return {
      messages: build_query_index(log).query(query),
      diagnostics: {
        total_chunks: 0,
        candidate_chunks: 0,
        matching_messages,
        used_file_indexes: false,
      },
    }
  }
  let records = scan_file(bytes, limits~)
  let by_offset : Map[UInt64, Record] = {}
  let declarations = empty_log()
  for record in records {
    by_offset[record.offset.to_uint64()] = record
    match record.opcode {
      0x03 => add_schema(declarations, decode_schema(record.body, limits~))
      0x04 => add_channel(declarations, decode_channel(record.body, limits~))
      _ => ()
    }
  }
  let allowed : Map[Int, Bool] = {}
  for channel in declarations.channels {
    if query.topics.is_empty() || query.topics.contains(channel.topic) {
      allowed[channel.id] = true
    }
  }
  let selected : Array[(Message, UInt64, Int)] = []
  let mut candidate_chunks = 0
  for index in indexes {
    if index.end_time < query.start_time || index.start_time > query.end_time {
      continue
    }
    if !index.message_index_offsets.is_empty() &&
      !index.message_index_offsets.any(fn(pair) { allowed.contains(pair.0) }) {
      continue
    }
    candidate_chunks += 1
    let record = match by_offset.get(index.chunk_start) {
      Some(value) => value
      None => raise Invalid("missing indexed chunk")
    }
    let chunk = decode_chunk(record.body, limits~)
    for nested in chunk.records {
      if nested.opcode != 0x05 {
        continue
      }
      let message = decode_message(nested.body, limits~)
      if allowed.contains(message.channel_id) &&
        message.log_time >= query.start_time &&
        message.log_time <= query.end_time {
        if selected.length() >= limits.max_records {
          raise Invalid("query message budget exceeded")
        }
        selected.push((message, index.chunk_start, nested.offset))
      }
    }
  }
  selected.sort_by(fn(a, b) {
    if a.0.log_time < b.0.log_time {
      -1
    } else if a.0.log_time > b.0.log_time {
      1
    } else if a.1 < b.1 {
      -1
    } else if a.1 > b.1 {
      1
    } else {
      a.2.compare(b.2)
    }
  })
  let begin = query.offset.min(selected.length())
  let count = query.limit.min(selected.length() - begin)
  let messages = []
  for i = begin; i < begin + count; i = i + 1 {
    let index = if query.descending { selected.length() - 1 - i } else { i }
    let value = selected[index].0
    messages.push({ ..value, data: value.data.copy(), })
  }
  {
    messages,
    diagnostics: {
      total_chunks: indexes.length(),
      candidate_chunks,
      matching_messages: selected.length(),
      used_file_indexes: true,
    },
  }
}
