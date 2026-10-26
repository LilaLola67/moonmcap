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
      "decode INPUT... [--where JSON --select /field --offset N --limit N --index SIDECAR --output NEW.ndjson --int64-strings] | raw DESCRIPTOR PAYLOAD TYPE [--int64-strings]",
    )
    return
  }
  let clean = []
  let mut strings = false
  for value in values {
    if value == "--int64-strings" {
      if strings {
        raise @mcap.io_error("duplicate int64 mode")
      }
      strings = true
    } else {
      clean.push(value)
    }
  }
  let options = { ..@mcap.protobuf_options(), int64_as_string: strings, }
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
  let decoder = @mcap.standard_payload_decoder(options~)
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
