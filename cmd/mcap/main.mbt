///|
extern "js" fn arguments() -> Array[String] =
  #| () => process.argv.slice(2)

///|
extern "js" fn error_exit(message : String, code : Int) -> Unit =
  #| (message,code) => { process.stderr.write(message+'\n'); process.exitCode=code; }

///|
extern "js" fn set_exit_code(code : Int) -> Unit =
  #| code => { process.exitCode=code; }

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
      "external-merge INPUT... --temp-dir DIR --output NEW.mcap [--run-bytes N --merge-bytes N --fan-in N --spool-bytes N --run-limit N --declaration-bytes N]",
    )
    println(
      "MoonMCAP: inspect|query|slice|merge|recover|rewrite|transform INPUT... [--output PATH]\n--topic TOPIC (repeatable) --start NS --end NS --offset N --limit N --descending\n--compression none|lz4|zstd --format json|csv --max-file-bytes N --cache-bytes N\n--chunk-messages N --full\ntransform: --rename FROM=TO --sample-period NS --sample-origin NS --align-start NS\n--attachment-name NAME --attachment-media-type TYPE --attachment-start NS --attachment-end NS\n--metadata-name NAME --metadata-match KEY=VALUE\nTime bounds are inclusive. Existing output paths are never overwritten.",
    )
    println(
      "validate INPUT [--output REPORT.json]: full structured diagnosis, exit 1 for invalid files\nreindex INPUT --output NEW.mcap: rebuild only from a complete valid data section",
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
    let messages = reader
      .cursor(options.query)
      .next_page(options.query.limit.max(1))
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
  if options.command == "external-merge" {
    let sources = options.inputs.map(fn(path) { @nodeio.open_file(path) })
    let transaction = @nodeio.output_transaction(options.output)
    try {
      let storage = @nodeio.sort_directory(options.temp_dir)
      let report = @mcap.external_merge(
        sources,
        storage,
        transaction.sink(),
        options=options.external,
      )
      transaction.publish()
      println(@mcap.external_report_json(report).stringify())
    } catch {
      error => {
        transaction.abort() catch {
          @mcap.Invalid(reason) =>
            raise @mcap.io_error("external output cleanup failed: \{reason}")
        }
        raise error
      }
    }
    return
  }
  let bytes = @nodeio.read_file(
    options.inputs[0],
    maximum=options.max_file_bytes,
  )
  if options.command == "validate" {
    let report = @mcap.diagnose_file(bytes)
    text_output(options.output, @mcap.diagnosis_json(report).stringify())
    if !report.valid {
      set_exit_code(1)
    }
    return
  }
  if options.command == "reindex" {
    let repair = @mcap.rebuild_indexes(
      bytes,
      compression=options.compression,
      messages_per_chunk=options.chunk_messages,
    )
    @nodeio.write_atomic_new(options.output, repair.bytes)
    let report : Json = {
      "original": @mcap.diagnosis_json(repair.original),
      "result": @mcap.diagnosis_json(repair.result),
    }
    println(report.stringify())
    return
  }
  let log = if options.command == "recover" {
    let report = @mcap.recover_prefix(bytes)
    let detail : Json = {
      "recovered_messages": report.log.messages.length(),
      "stop_offset": report.stop_offset,
      "complete": report.complete,
      "reason": report.reason,
      "source_diagnosis": @mcap.diagnosis_json(@mcap.diagnose_file(bytes)),
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
    "transform" => @mcap.transform_log(log, options.transform)
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
