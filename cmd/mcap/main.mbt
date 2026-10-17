///|
extern "js" fn arguments() -> Array[String] =
  #| () => process.argv.slice(2)

///|
extern "js" fn error_exit(message : String, code : Int) -> Unit =
  #| (message,code) => { process.stderr.write(message+'\n'); process.exitCode=code; }

///|
fn text_output(path : String, text : String) -> Unit raise @mcap.McapError {
  if path == "" {
    println(text)
  } else {
    @nodeio.write_atomic_new(path, @utf8.encode(text[:]))
  }
}

///|
fn run(options : @mcap.CommandOptions) -> Unit raise @mcap.McapError {
  if options.command == "help" {
    println(
      "MoonMCAP: inspect|query|slice|merge|recover|rewrite INPUT... [--output PATH]\n--topic TOPIC (repeatable) --start NS --end NS --offset N --limit N --descending\n--compression none|lz4|zstd --format json|csv --max-file-bytes N --cache-bytes N\n--chunk-messages N --full\nTime bounds are inclusive. Existing output paths are never overwritten.",
    )
    return
  }
  if options.command == "query" {
    let reader = @mcap.open_reader(
      @nodeio.open_file(options.inputs[0]),
      cache_bytes=options.cache_bytes,
      fallback_bytes=options.max_file_bytes,
    )
    if options.full {
      reader.validate_all()
    }
    let messages = reader.query(options.query)
    if options.format == "csv" {
      text_output(
        options.output,
        @mcap.messages_csv(messages, reader.channels()),
      )
    } else {
      let report : Json = {
        "messages": @mcap.messages_json(messages, reader.channels()),
        "diagnostics": @mcap.read_diagnostics_json(reader.diagnostics()),
      }
      text_output(options.output, report.stringify())
    }
    return
  }
  let bytes = @nodeio.read_file(
    options.inputs[0],
    maximum=options.max_file_bytes,
  )
  let log = if options.command == "recover" {
    let report = @mcap.recover_prefix(bytes)
    let detail : Json = {
      "recovered_messages": report.log.messages.length(),
      "stop_offset": report.stop_offset,
      "complete": report.complete,
      "reason": report.reason,
    }
    error_exit(detail.stringify(), 0)
    report.log
  } else {
    @mcap.read_log(bytes)
  }
  if options.command == "inspect" {
    text_output(options.output, @mcap.inspect_json(log).stringify())
    return
  }
  let result = match options.command {
    "slice" => @mcap.slice_log(log, options.query)
    "merge" => {
      let logs = [log]
      let mut loaded = bytes.length()
      for i = 1; i < options.inputs.length(); i = i + 1 {
        let next = @nodeio.read_file(
          options.inputs[i],
          maximum=options.max_file_bytes - loaded,
        )
        loaded += next.length()
        logs.push(@mcap.read_log(next))
      }
      @mcap.merge_logs(logs)
    }
    _ => log
  }
  let output = @mcap.write_indexed(
    result,
    compression=options.compression,
    messages_per_chunk=options.chunk_messages,
  )
  @nodeio.write_atomic_new(options.output, output)
  println(@mcap.inspect_json(result).stringify())
}

///|
fn main {
  run(@mcap.parse_command(arguments())) catch {
    @mcap.Invalid(reason) => {
      let error : Json = { "error": reason }
      error_exit(error.stringify(), 2)
    }
  }
}
