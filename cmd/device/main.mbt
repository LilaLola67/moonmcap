///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn input_lines() -> () -> String =
  #| ()=>{const fs=process.getBuiltinModule('node:fs');let pending=Buffer.alloc(0),ended=false;return ()=>{try{for(;;){const n=pending.indexOf(10);if(n>=0){if(n>2101248)throw new Error('device line exceeds budget');const line=pending.subarray(0,n);pending=pending.subarray(n+1);return '+'+new TextDecoder('utf-8',{fatal:true}).decode(line);}if(pending.length>2101248)throw new Error('device line exceeds budget');if(ended){if(pending.length)throw new Error('device input must end with newline');return '';}const b=Buffer.alloc(65536);const size=fs.readSync(0,b,0,b.length,null);if(!size)ended=true;else pending=Buffer.concat([pending,b.subarray(0,size)]);}}catch(e){return '!'+e.message;}};}

///|
extern "js" fn error_text(reason : String) -> Unit =
  #| reason=>{process.stderr.write(reason+'\n');process.exitCode=2;}

///|
fn object(
  value : Json,
  allowed : Array[String],
) -> Map[String, Json] raise @mcap.McapError {
  guard value is Object(fields) else {
    raise @mcap.io_error("device JSON object required")
  }
  for key in fields.keys() {
    if !allowed.contains(key) {
      raise @mcap.io_error("unknown device key: " + key)
    }
  }
  fields
}

///|
fn text(
  fields : Map[String, Json],
  key : String,
) -> String raise @mcap.McapError {
  guard fields.get(key) is Some(String(s)) else {
    raise @mcap.io_error("device string required: " + key)
  }
  s
}

///|
fn parse(data : Bytes) -> Json raise @mcap.McapError {
  guard @mcap.json_filter("true", max_bytes=4 * 1024 * 1024).project(data)
    is Object(fields) else {
    raise @mcap.io_error("invalid device JSON")
  }
  fields["data"]
}

///|
fn run(values : Array[String]) -> Unit raise @mcap.McapError {
  if values.length() < 2 ||
    values.length() > 3 ||
    (values.length() == 3 && values[2] != "--resume") {
    raise @mcap.io_error(
      "device CONFIG.json RECORDING_DIR [--resume]; newline envelopes on stdin",
    )
  }
  let config = object(
    parse(@nodeio.read_file(values[0], maximum=4 * 1024 * 1024)),
    ["channels", "segment_messages", "identity"],
  )
  guard config.get("channels") is Some(Array(channels)) &&
    !channels.is_empty() &&
    channels.length() <= 128 else {
    raise @mcap.io_error("1..128 device channels required")
  }
  let builder = @mcap.new_builder()
  let ids : Map[String, Int32] = {}
  let json_channels : Map[String, Bool] = {}
  for value in channels {
    let channel = object(value, [
      "topic", "message_encoding", "schema", "metadata",
    ])
    let topic = text(channel, "topic")
    let encoding = text(channel, "message_encoding")
    if topic == "" || ids.contains(topic) {
      raise @mcap.io_error("unique nonempty device topics required")
    }
    let schema_id = match channel.get("schema") {
      None => 0
      Some(value) => {
        let schema = object(value, ["name", "encoding", "data_hex"])
        builder.register_schema(
          text(schema, "name"),
          text(schema, "encoding"),
          @mcap.decode_hex(text(schema, "data_hex"), max_bytes=1024 * 1024),
        )
      }
    }
    let metadata = match channel.get("metadata") {
      None => []
      Some(Object(entries)) =>
        entries
        .to_array()
        .map(fn(pair) {
          guard pair.1 is String(s) else {
            raise @mcap.io_error("device metadata strings required")
          }
          (pair.0, s)
        })
      _ => raise @mcap.io_error("device metadata object required")
    }
    ids[topic] = builder.register_channel(
      topic,
      encoding,
      schema_id~,
      metadata~,
    )
    json_channels[topic] = encoding == "json"
  }
  let segment_messages = match config.get("segment_messages") {
    None => 10000
    Some(Number(_, ..) as n) => {
      let value = @mcap.parse_decimal(n.stringify())
      if value < 1UL || value > 50000UL {
        raise @mcap.io_error("device segment_messages must be 1..50000")
      }
      value.to_int()
    }
    _ => raise @mcap.io_error("device segment_messages integer required")
  }
  let directory = @nodeio.open_recording_directory(
    values[1],
    resume=values.length() == 3,
  )
  defer directory.close()
  let session = @mcap.open_recording_session(
    builder.snapshot(),
    directory.storage(),
    manifest?=directory.manifest(),
    options={
      ..@mcap.recording_options(),
      segment_messages,
      source_identity: text(config, "identity"),
    },
  )
  let next = input_lines()
  println(({ "kind": "ready" } : Json).stringify())
  try {
    while true {
      let line = next()
      if line == "" {
        break
      }
      if line.has_prefix("!") {
        raise @mcap.io_error(line)
      }
      let fields = object(parse(@utf8.encode(line[1:][:])), [
        "topic", "log_time", "publish_time", "sequence", "data_hex", "value",
      ])
      let topic = text(fields, "topic")
      guard ids.get(topic) is Some(channel_id) else {
        raise @mcap.io_error("unregistered device topic")
      }
      if fields.contains("data_hex") == fields.contains("value") {
        raise @mcap.io_error(
          "device envelope requires exactly data_hex or value",
        )
      }
      let data = if fields.contains("data_hex") {
        @mcap.decode_hex(text(fields, "data_hex"), max_bytes=1024 * 1024)
      } else {
        if !json_channels[topic] {
          raise @mcap.io_error("value is only accepted on JSON channels")
        }
        @utf8.encode(fields["value"].stringify()[:])
      }
      if data.length() > 1024 * 1024 {
        raise @mcap.io_error("device payload exceeds 1 MiB")
      }
      let sequence = @mcap.parse_decimal(text(fields, "sequence"))
      if sequence > 4294967295UL {
        raise @mcap.io_error("device sequence exceeds UInt32")
      }
      session.append({
        channel_id,
        sequence: sequence.to_uint(),
        log_time: @mcap.parse_decimal(text(fields, "log_time")),
        publish_time: @mcap.parse_decimal(text(fields, "publish_time")),
        data,
      })
      println(
        (
          {
            "kind": "accepted",
            "committed_messages": session.committed_messages().to_string(),
          } : Json).stringify(),
      )
    }
    let report = session.finish()
    println(({ "kind": "finished", "recording": report } : Json).stringify())
  } catch {
    error => {
      session.cancel()
      raise error
    }
  }
}

///|
fn main {
  run(args()) catch {
    @mcap.Invalid(reason) => error_text(reason)
  }
}
