///|
pub fn messages_json(
  messages : Array[Message],
  channels : Array[Channel],
) -> Json {
  Json::array(
    messages.map(fn(m) {
      let topic = channels
        .iter()
        .find_first(fn(c) { c.id == m.channel_id })
        .map(fn(c) { c.topic })
        .unwrap_or("")
      {
        "channel_id": m.channel_id,
        "topic": topic,
        "sequence": m.sequence,
        "log_time": m.log_time.to_string(),
        "publish_time": m.publish_time.to_string(),
        "data_hex": encode_hex(m.data),
      }
    }),
  )
}

///|
fn csv_field(value : String) -> String {
  "\"" + value.replace_all(old="\"", new="\"\"") + "\""
}

///|
pub fn messages_csv(
  messages : Array[Message],
  channels : Array[Channel],
) -> String {
  let out = StringBuilder::StringBuilder()
  out.write_string(
    "channel_id,topic,sequence,log_time,publish_time,data_hex\r\n",
  )
  for message in messages {
    let topic = channels
      .iter()
      .find_first(fn(c) { c.id == message.channel_id })
      .map(fn(c) { c.topic })
      .unwrap_or("")
    out.write_string(
      "\{message.channel_id},\{csv_field(topic)},\{message.sequence},\{message.log_time},\{message.publish_time},\{encode_hex(message.data)}\r\n",
    )
  }
  out.to_string()
}

///|
pub fn read_diagnostics_json(d : ReadDiagnostics) -> Json {
  {
    "read_calls": d.read_calls,
    "bytes_read": d.bytes_read.to_string(),
    "decoded_chunks": d.decoded_chunks,
    "cache_hits": d.cache_hits,
    "cache_bytes": d.cache_bytes,
    "peak_cache_bytes": d.peak_cache_bytes,
    "indexed": d.indexed,
    "full_validation": d.full_validation,
  }
}

///|
pub fn inspect_json(log : Log) -> Json raise McapError {
  let stats = summarize(log)
  {
    "profile": log.header.profile,
    "library": log.header.library,
    "schemas": log.schemas.length(),
    "channels": log.channels.length(),
    "messages": stats.message_count,
    "payload_bytes": stats.payload_bytes.to_string(),
    "attachments": log.attachments.length(),
    "metadata": log.metadata.length(),
    "topics": stats.topics.map(fn(t) -> Json {
      {
        "topic": t.topic,
        "channel_id": t.channel_id,
        "messages": t.message_count,
        "first_time": t.first_time.map(fn(v) { v.to_string() }),
        "last_time": t.last_time.map(fn(v) { v.to_string() }),
      }
    }),
  }
}
