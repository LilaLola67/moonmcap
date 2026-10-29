///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn error_text(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
fn object(
  value : Json,
  allowed : Array[String],
) -> Map[String, Json] raise @mcap.McapError {
  guard value is Object(fields) else {
    raise @mcap.io_error("configuration object expected")
  }
  for key in fields.keys() {
    if !allowed.contains(key) {
      raise @mcap.io_error("unknown configuration key: " + key)
    }
  }
  fields
}

///|
fn text(
  fields : Map[String, Json],
  key : String,
) -> String raise @mcap.McapError {
  guard fields.get(key) is Some(String(value)) else {
    raise @mcap.io_error("string expected: " + key)
  }
  value
}

///|
fn json_file(path : String) -> Json raise @mcap.McapError {
  let wrapped = @mcap.json_filter("true").project(
    @nodeio.read_file(path, maximum=1024 * 1024),
  )
  guard wrapped is Object(fields) else {
    raise @mcap.io_error("invalid JSON document")
  }
  fields["data"]
}

///|
fn boolean(
  fields : Map[String, Json],
  key : String,
  fallback : Bool,
) -> Bool raise @mcap.McapError {
  match fields.get(key) {
    None => fallback
    Some(True) => true
    Some(False) => false
    _ => raise @mcap.io_error("boolean expected: " + key)
  }
}

///|
fn rules(
  path : String,
) -> (Map[String, Json], Array[@mcap.PayloadEdit], Array[String]) raise @mcap.McapError {
  let config = object(json_file(path), [
    "edits", "topics", "copy_auxiliary", "input_records",
  ])
  guard config.get("edits") is Some(Array(items)) else {
    raise @mcap.io_error("edits array required")
  }
  let edits : Array[@mcap.PayloadEdit] = items.map(fn(item) {
    let f = object(item, ["path", "value", "remove"])
    let remove = boolean(f, "remove", false)
    if remove == f.contains("value") {
      raise @mcap.io_error("edit requires exactly one of value or remove:true")
    }
    {
      path: text(f, "path"),
      replacement: if remove {
        None
      } else {
        Some(f["value"])
      },
    }
  })
  let topics = match config.get("topics") {
    None => []
    Some(Array(items)) =>
      items.map(fn(item) {
        match item {
          String(topic) if topic != "" => topic
          _ => raise @mcap.io_error("nonempty topic string required")
        }
      })
    _ => raise @mcap.io_error("topics array expected")
  }
  (config, edits, topics)
}

///|
fn encoding(format : String) -> String raise @mcap.McapError {
  match format {
    "protobuf" => "protobuf"
    "cdr" => "ros2msg"
    "ros1" => "ros1msg"
    _ => raise @mcap.io_error("format must be protobuf, cdr or ros1")
  }
}

///|
fn run(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  if values.is_empty() || values == ["--help"] {
    println(
      "payload encode FORMAT SCHEMA TYPE VALUE.json NEW.bin [--big-endian] | raw-rewrite FORMAT SCHEMA TYPE INPUT.bin RULES.json NEW.bin | rewrite INPUT.mcap RULES.json NEW.mcap | compare FORMAT OLD_SCHEMA OLD_TYPE NEW_SCHEMA NEW_TYPE | normalize INPUT.mcap RULES.json NEW.ndjson | sync INPUT.mcap RULES.json NEW.ndjson; see docs/payload-rewrite.md and docs/schema-evolution.md",
    )
    return
  }
  if (values[0] == "tf-query" || values[0] == "tf-clip") && values.length() == 4 {
    tf_file(values, scope)
    return
  }
  if values[0] == "tail" && values.length() == 4 {
    tail_file(values, scope)
    return
  }
  if values[0] == "compare" && values.length() == 6 {
    let schema_encoding = encoding(values[1])
    let old : @mcap.Schema = {
      id: 1,
      name: values[3],
      encoding: schema_encoding,
      data: @nodeio.read_file(values[2], maximum=4 * 1024 * 1024),
    }
    let new : @mcap.Schema = {
      id: 1,
      name: values[5],
      encoding: schema_encoding,
      data: @nodeio.read_file(values[4], maximum=4 * 1024 * 1024),
    }
    println(@mcap.compare_payload_schemas(old, new).stringify())
    return
  }
  if values[0] == "sync" && values.length() == 4 {
    sync_file(values, scope)
    return
  }
  if values[0] == "normalize" && values.length() == 4 {
    normalize_file(values, scope)
    return
  }
  if values[0] == "encode" && (values.length() == 6 || values.length() == 7) {
    if values.length() == 7 &&
      (values[1] != "cdr" || values[6] != "--big-endian") {
      raise @mcap.io_error("big-endian flag is only valid for CDR encode")
    }
    let format = values[1]
    ignore(encoding(format))
    let schema = @nodeio.read_file(values[2], maximum=4 * 1024 * 1024)
    let value = json_file(values[4])
    let bytes = match format {
      "protobuf" => @mcap.protobuf_decoder(schema).encode(values[3], value)
      "cdr" =>
        @mcap.cdr_decoder(values[3], schema).encode(
          value,
          little_endian=values.length() == 6,
        )
      _ => @mcap.ros1_decoder(values[3], schema).encode(value)
    }
    @nodeio.write_atomic_new(values[5], bytes)
    println(
      ({ "bytes": bytes.length(), "output": values[5] } : Json).stringify(),
    )
    return
  }
  if values[0] == "raw-rewrite" && values.length() == 7 {
    let format = values[1]
    let schema_encoding = encoding(format)
    let schema : @mcap.Schema = {
      id: 1,
      name: values[3],
      encoding: schema_encoding,
      data: @nodeio.read_file(values[2], maximum=4 * 1024 * 1024),
    }
    let channel : @mcap.Channel = {
      id: 1,
      topic: "/payload",
      message_encoding: format,
      schema_id: 1,
      metadata: [],
    }
    let (config, edits, _) = rules(values[5])
    if config.keys().any(fn(key) { key != "edits" }) {
      raise @mcap.io_error("raw rewrite configuration accepts only edits")
    }
    let bytes = @mcap.standard_payload_rewriter().rewrite(
      Some(schema),
      channel,
      @nodeio.read_file(values[4], maximum=16 * 1024 * 1024),
      edits,
    )
    @nodeio.write_atomic_new(values[6], bytes)
    println(
      ({ "bytes": bytes.length(), "output": values[6] } : Json).stringify(),
    )
    return
  }
  if values[0] != "rewrite" || values.length() != 4 {
    raise @mcap.io_error("invalid payload command")
  }
  let (config, edits, topics) = rules(values[2])
  let records = match config.get("input_records") {
    None => 30000000
    Some(Number(_, ..) as value) => {
      let n = @mcap.parse_decimal(value.stringify())
      if n < 1UL || n > 2147483647UL {
        raise @mcap.io_error("invalid input_records budget")
      }
      n.to_int()
    }
    _ => raise @mcap.io_error("input_records integer expected")
  }
  let copy_auxiliary = boolean(config, "copy_auxiliary", true)
  let source = scope.open(values[1])
  let sha256 = @nodeio.source_sha256(source)
  let transaction = @nodeio.output_transaction(values[3])
  let report = try {
    let report = @mcap.stream_rewrite_payload(
      source,
      transaction.sink(),
      edits,
      topics~,
      copy_auxiliary~,
      options={ ..@mcap.stream_options(), compression: @mcap.Zstd, },
      limits={ ..@mcap.default_limits(), max_records: records, },
    )
    transaction.publish()
    report
  } catch {
    error => {
      transaction.abort() catch {
        _ => ()
      }
      raise error
    }
  }
  println(
    ({ "source_sha256": sha256, "output": values[3], "report": report } : Json).stringify(),
  )
}

///|
fn main {
  let scope = @nodeio.file_scope()
  run(args(), scope) catch {
    @mcap.Invalid(reason) => error_text(reason)
  }
  scope.close() catch {
    @mcap.Invalid(reason) => error_text(reason)
  }
}
