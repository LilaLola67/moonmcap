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
extern "js" fn stdout_text(text : String) -> Unit =
  #| text=>{process.stdout.write(text);}

///|
fn text_output(path : String, text : String) -> Unit raise @mcap.McapError {
  if path == "" {
    println(text)
  } else {
    @nodeio.write_atomic_new(path, @utf8.encode(text[:]))
  }
}

///|
fn indexed_reader(
  path : String,
  index : String,
  options : @mcap.CommandOptions,
) -> @mcap.SidecarReader raise @mcap.McapError {
  let source = @nodeio.open_file(path)
  let verification = if options.fast_index {
    @mcap.TrustIdentity(@nodeio.file_identity_token(path))
  } else {
    @mcap.VerifyContent(@nodeio.source_sha256)
  }
  let reader = @mcap.open_sidecar(
    source,
    @nodeio.read_file(index, maximum=options.index_bytes),
    verification,
    index_bytes=options.index_bytes,
    cache_bytes=options.cache_bytes,
  )
  if options.full {
    ignore(@mcap.scan_source(source, @mcap.source_visitor()))
  }
  reader
}

///|
fn run(options : @mcap.CommandOptions) -> Unit raise @mcap.McapError {
  if options.command == "help" {
    println(
      "external-merge INPUT... --job NEW_DIR [--resume] --output NEW.mcap; job-inspect DIR; job-clean DIR",
    )
    println(
      "JSON queries: --where '{\"path\":\"/speed\",\"op\":\">\",\"value\":80}' --select /speed (repeatable) --skip-invalid-json; also stream-transform --where",
    )
    println(
      "index-build|index-rebuild INPUT --output NEW.midx; index-check|index-query INPUT --index INDEX [--fast-index --index-bytes N]; dataset-query accepts one --index per input",
    )
    println(
      "dataset-query INPUT... [--topic TOPIC --start NS --end NS --offset N --limit N --descending --format json|ndjson]: query files without merging output",
    )
    println(
      "stream-transform INPUT --temp-dir DIR --output NEW.mcap: bounded sorting, sampling and transformation; query --format ndjson streams one message per line",
    )
    println(
      "recover-blocks INPUT --output NEW.mcap: explicitly extract CRC-verified indexed chunks and print a partial-recovery manifest",
    )
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
  if ["job-inspect", "job-clean"].contains(options.command) {
    let job = @nodeio.open_merge_job(options.inputs[0], resume=true)
    try {
      if options.command == "job-clean" {
        job.clean()
        println("{\"cleaned\":true}")
      } else {
        text_output(options.output, job.inspect())
        job.close()
      }
    } catch {
      error => {
        job.close() catch {
          _ => ()
        }
        raise error
      }
    }
    return
  }
  if options.command == "external-merge" && options.job_dir != "" {
    let job = @nodeio.open_merge_job(options.job_dir, resume=options.resume_job)
    try {
      let checkpoint = job.checkpoint()
      let sources = options.inputs.map(fn(path) { @nodeio.open_file(path) })
      let transaction = job.output(options.output)
      try {
        let report = @mcap.resumable_merge(
          sources,
          job.storage(),
          transaction.sink(),
          @nodeio.source_sha256,
          fn(bytes) { job.save(bytes) },
          checkpoint?,
          options=options.external,
        )
        transaction.publish()
        let json = @mcap.external_report_json(report)
        job.complete(json)
        println(json.stringify())
      } catch {
        error => {
          transaction.abort()
          raise error
        }
      }
      job.close()
    } catch {
      error => {
        job.close() catch {
          _ => ()
        }
        raise error
      }
    }
    return
  }
  if options.content is Some(filter) &&
    ["query", "index-query", "dataset-query"].contains(options.command) {
    let sources = Array::makei(options.inputs.length(), fn(i) {
      let provider = if !options.indexes.is_empty() {
        indexed_reader(options.inputs[i], options.indexes[i], options).query_source()
      } else {
        let reader = @mcap.open_reader(
          @nodeio.open_file(options.inputs[i]),
          cache_bytes=options.cache_bytes,
          fallback_bytes=options.max_file_bytes,
        )
        if options.full {
          reader.validate_all()
        }
        reader.query_source()
      }
      provider.filter_json(filter)
    })
    let dataset = @mcap.open_dataset(sources)
    let cursor = dataset.cursor(options.query)
    if options.format == "ndjson" {
      let transaction = if options.output == "" {
        None
      } else {
        Some(@nodeio.output_transaction(options.output))
      }
      try {
        while cursor.next_record() is Some(record) {
          let text = @mcap.json_query_record(record, dataset.channels(), filter).stringify() +
            "\n"
          match transaction {
            Some(value) => value.sink().write(@utf8.encode(text[:]))
            None => stdout_text(text)
          }
        }
        if transaction is Some(value) {
          value.publish()
        }
      } catch {
        error => {
          cursor.cancel()
          if transaction is Some(value) {
            value.abort()
          }
          raise error
        }
      }
    } else {
      let records = cursor.next_page(options.query.limit.max(1))
      let report : Json = {
        "messages": records.map(fn(record) {
          @mcap.json_query_record(record, dataset.channels(), filter)
        }),
        "source_count": sources.length(),
      }
      text_output(options.output, report.stringify())
    }
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
    let cursor = reader.cursor(options.query)
    if options.format == "ndjson" {
      if options.output == "" {
        ignore(
          cursor.write_ndjson(
            @mcap.new_write_sink(fn(bytes) {
              stdout_text(
                @utf8.decode(bytes[:]) catch {
                  _ => raise @mcap.io_error("invalid NDJSON UTF-8")
                },
              )
            }),
          ),
        )
      } else {
        let transaction = @nodeio.output_transaction(options.output)
        try {
          ignore(cursor.write_ndjson(transaction.sink()))
          transaction.publish()
        } catch {
          error => {
            transaction.abort()
            raise error
          }
        }
      }
      return
    }
    let messages = cursor.next_page(options.query.limit.max(1))
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
  if ["index-build", "index-rebuild"].contains(options.command) {
    let source = @nodeio.open_file(options.inputs[0])
    let token = @nodeio.file_identity_token(options.inputs[0])
    let bytes = @mcap.build_sidecar(
      source,
      @nodeio.source_sha256,
      identity=token,
      index_bytes=options.index_bytes,
    )
    if @nodeio.file_identity_token(options.inputs[0]) != token {
      raise @mcap.io_error("source identity changed while indexing")
    }
    let reader = @mcap.open_sidecar(
      source,
      bytes,
      @mcap.TrustIdentity(token),
      index_bytes=options.index_bytes,
    )
    @nodeio.write_atomic_new(options.output, bytes)
    let report : Json = { "build_content_hashed": true, "index": reader.info() }
    println(report.stringify())
    return
  }
  if ["index-check", "index-query"].contains(options.command) {
    let reader = indexed_reader(options.inputs[0], options.indexes[0], options)
    if options.command == "index-check" {
      reader.validate_entries()
      text_output(options.output, reader.info().stringify())
    } else {
      let cursor = reader.cursor(options.query)
      if options.format == "ndjson" {
        if options.output == "" {
          ignore(
            cursor.write_ndjson(
              @mcap.new_write_sink(fn(bytes) {
                stdout_text(
                  @utf8.decode(bytes[:]) catch {
                    _ => raise @mcap.io_error("invalid NDJSON UTF-8")
                  },
                )
              }),
            ),
          )
        } else {
          let transaction = @nodeio.output_transaction(options.output)
          try {
            ignore(cursor.write_ndjson(transaction.sink()))
            transaction.publish()
          } catch {
            error => {
              transaction.abort()
              raise error
            }
          }
        }
      } else {
        let messages = cursor.next_page(options.query.limit.max(1))
        let report : Json = {
          "messages": @mcap.messages_json(messages, reader.channels()),
          "index": reader.info(),
        }
        text_output(options.output, report.stringify())
      }
    }
    return
  }
  if options.command == "dataset-query" {
    let sources = Array::makei(options.inputs.length(), fn(i) {
      let path = options.inputs[i]
      if !options.indexes.is_empty() {
        return indexed_reader(path, options.indexes[i], options).query_source()
      }
      let reader = @mcap.open_reader(
        @nodeio.open_file(path),
        cache_bytes=options.cache_bytes,
        fallback_bytes=options.max_file_bytes,
      )
      if options.full {
        reader.validate_all()
      }
      reader.query_source()
    })
    let dataset = @mcap.open_dataset(sources)
    let cursor = dataset.cursor(options.query)
    if options.format == "ndjson" {
      if options.output == "" {
        ignore(
          cursor.write_ndjson(
            @mcap.new_write_sink(fn(bytes) {
              stdout_text(
                @utf8.decode(bytes[:]) catch {
                  _ => raise @mcap.io_error("invalid NDJSON UTF-8")
                },
              )
            }),
          ),
        )
      } else {
        let transaction = @nodeio.output_transaction(options.output)
        try {
          ignore(cursor.write_ndjson(transaction.sink()))
          transaction.publish()
        } catch {
          error => {
            transaction.abort()
            raise error
          }
        }
      }
    } else {
      let messages = cursor.next_page(options.query.limit.max(1))
      let report : Json = {
        "messages": messages.map(fn(record) {
          @mcap.dataset_message_json(record, dataset.channels())
        }),
        "source_count": sources.length(),
        "materialized_sources": cursor.progress().materialized_sources,
      }
      text_output(options.output, report.stringify())
    }
    return
  }
  if options.command == "recover-blocks" {
    let source = @nodeio.open_file(options.inputs[0])
    let transaction = @nodeio.output_transaction(options.output)
    try {
      let report = @mcap.recover_blocks(source, transaction.sink(), options={
        ..@mcap.stream_options(),
        compression: options.compression,
        chunk_messages: options.chunk_messages,
      })
      transaction.publish()
      println(@mcap.block_recovery_json(report).stringify())
    } catch {
      error => {
        transaction.abort()
        raise error
      }
    }
    return
  }
  if options.command == "stream-transform" {
    let source = @nodeio.open_file(options.inputs[0])
    let transaction = @nodeio.output_transaction(options.output)
    try {
      let report = @mcap.stream_transform(
        source,
        @nodeio.sort_directory(options.temp_dir),
        transaction.sink(),
        options.transform,
        external=options.external,
        content?=options.content,
      )
      transaction.publish()
      println(@mcap.transform_report_json(report).stringify())
    } catch {
      error => {
        transaction.abort()
        raise error
      }
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
