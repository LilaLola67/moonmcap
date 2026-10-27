///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn report_error(message : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
fn run(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  if values.is_empty() || values == ["--help"] {
    println(
      "decode INPUT... [--where JSON --select /field --offset N --limit N --index SIDECAR --output NEW.ndjson --int64-strings --protojson --emit-defaults] | raw DESCRIPTOR PAYLOAD TYPE | raw-cdr ROS2MSG PAYLOAD TYPE",
    )
    return
  }
  let clean = []
  let mut strings = false
  let mut canonical = false
  let mut defaults = false
  for value in values {
    if value == "--protojson" {
      if canonical {
        raise @mcap.io_error("duplicate ProtoJSON option")
      }
      canonical = true
    } else if value == "--emit-defaults" {
      if defaults {
        raise @mcap.io_error("duplicate default option")
      }
      defaults = true
    } else if value == "--int64-strings" {
      if strings {
        raise @mcap.io_error("duplicate int64 mode")
      }
      strings = true
    } else {
      clean.push(value)
    }
  }
  if defaults && !canonical {
    raise @mcap.io_error("emit-defaults requires ProtoJSON mode")
  }
  if clean.is_empty() {
    raise @mcap.io_error("decode requires an input")
  }
  let cdr = { ..@mcap.cdr_options(), int64_as_string: strings, }
  if clean[0] == "raw-ros1" {
    if clean.length() != 4 {
      raise @mcap.io_error(
        "raw-ros1 requires ros1msg definition, payload and type",
      )
    }
    println(
      @mcap.ros1_decoder(
        clean[3],
        @nodeio.read_file(clean[1], maximum=cdr.schema_bytes),
        options=cdr,
      )
      .decode(@nodeio.read_file(clean[2], maximum=cdr.message_bytes))
      .stringify(),
    )
    return
  }
  if clean[0] == "raw-cdr" {
    if clean.length() != 4 {
      raise @mcap.io_error(
        "raw-cdr requires ros2msg definition, payload and type",
      )
    }
    let decoder = @mcap.cdr_decoder(
      clean[3],
      @nodeio.read_file(clean[1], maximum=cdr.schema_bytes),
      options=cdr,
    )
    println(
      decoder
      .decode(@nodeio.read_file(clean[2], maximum=cdr.message_bytes))
      .stringify(),
    )
    return
  }
  let options = {
    ..@mcap.protobuf_options(),
    int64_as_string: strings,
    protojson: canonical,
    emit_defaults: defaults,
  }
  if clean[0] == "raw" {
    if clean.length() != 4 {
      raise @mcap.io_error(
        "raw requires descriptor, payload and fully qualified type",
      )
    }
    let decoder = @mcap.protobuf_decoder(
      @nodeio.read_file(clean[1], maximum=options.descriptor_bytes),
      options~,
    )
    println(
      decoder
      .decode(
        clean[3],
        @nodeio.read_file(clean[2], maximum=options.message_bytes),
      )
      .stringify(),
    )
    return
  }
  let parsed = @mcap.parse_command(["dataset-query"] + clean)
  if parsed.full {
    raise @mcap.io_error(
      "decode uses selective queries; use validate for complete source checking",
    )
  }
  if !parsed.indexes.is_empty() &&
    parsed.indexes.length() != parsed.inputs.length() {
    raise @mcap.io_error("provide one index per input")
  }
  let decoder = @mcap.standard_payload_decoder(options~, cdr~)
  let projection = match parsed.content {
    Some(filter) => filter
    None => @mcap.json_filter("true")
  }
  let providers = []
  for i = 0; i < parsed.inputs.length(); i = i + 1 {
    let source = scope.open(parsed.inputs[i])
    let provider = if parsed.indexes.is_empty() {
      @mcap.open_reader(
        source,
        cache_bytes=parsed.cache_bytes,
        fallback_bytes=parsed.max_file_bytes,
      ).query_source()
    } else {
      let index = scope.open(parsed.indexes[i])
      if index.read_exact(0UL, 8) == b"MIDX2\r\n\x00" {
        @mcap.open_disk_sidecar(
          source,
          index,
          @mcap.VerifyContent(@nodeio.source_sha256),
          chunk_cache_bytes=parsed.cache_bytes,
        ).query_source()
      } else {
        @mcap.open_sidecar(
          source,
          @nodeio.read_file(parsed.indexes[i], maximum=parsed.index_bytes),
          @mcap.VerifyContent(@nodeio.source_sha256),
          cache_bytes=parsed.cache_bytes,
        ).query_source()
      }
    }
    providers.push(provider.decode_payload(decoder).filter_json(projection))
  }
  let dataset = @mcap.open_dataset(providers)
  let cursor = dataset.cursor(parsed.query)
  let transaction = if parsed.output == "" {
    None
  } else {
    Some(@nodeio.output_transaction(parsed.output))
  }
  try {
    while cursor.next_record() is Some(record) {
      let line = @mcap.json_query_record(record, dataset.channels(), projection).stringify() +
        "\n"
      match transaction {
        None => println(line[0:line.length() - 1])
        Some(output) => output.sink().write(@utf8.encode(line[:]))
      }
    }
    if transaction is Some(output) {
      output.publish()
    }
  } catch {
    error => {
      cursor.cancel()
      if transaction is Some(output) {
        output.abort()
      }
      raise error
    }
  }
}

///|
fn main {
  let scope = @nodeio.file_scope()
  run(args(), scope) catch {
    @mcap.Invalid(reason) => report_error(reason)
  }
  scope.close() catch {
    @mcap.Invalid(reason) => report_error(reason)
  }
}
