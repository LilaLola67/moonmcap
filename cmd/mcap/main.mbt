///|
extern "js" fn host_arguments() -> FixedArray[String] =
  #| () => process.argv.slice(2)

///|
fn arguments() -> Array[String] {
  Array::from_fixed_array(host_arguments())
}

///|
extern "js" fn error_exit(message : String, code : Int32) -> Unit =
  #| (message,code) => { process.stderr.write(message+'\n'); process.exitCode=code; }

///|
extern "js" fn set_exit_code(code : Int32) -> Unit =
  #| code => { process.exitCode=code; }

///|
extern "js" fn stdout_text(text : String) -> Unit =
  #| text=>{process.stdout.write(text);}

///|
fn text_output(path : String, text : String) -> Unit raise @core.McapError {
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
  options : @core.CommandOptions,
  scope : @nodeio.FileScope,
) -> @core.SidecarReader raise @core.McapError {
  let source = scope.open(path)
  let verification = if options.fast_index {
    @core.TrustIdentity(@nodeio.file_identity_token(path))
  } else {
    @core.VerifyContent(@nodeio.source_sha256)
  }
  let reader = @core.open_sidecar(
    source,
    @nodeio.read_file(index, maximum=options.index_bytes),
    verification,
    index_bytes=options.index_bytes,
    cache_bytes=options.cache_bytes,
  )
  if options.full {
    ignore(@core.scan_source(source, @core.source_visitor()))
  }
  reader
}

///|
fn query_dataset(
  options : @core.CommandOptions,
  scope : @nodeio.FileScope,
) -> @core.Dataset raise @core.McapError {
  let sources = Array::makei(options.inputs.length(), i => {
    let provider = if options.indexes.is_empty() {
      let reader = @core.open_reader(
        scope.open(options.inputs[i]),
        cache_bytes=options.cache_bytes,
        fallback_bytes=options.max_file_bytes,
      )
      if options.full {
        reader.validate_all()
      }
      reader.query_source()
    } else {
      indexed_reader(options.inputs[i], options.indexes[i], options, scope).query_source()
    }
    match options.content {
      Some(filter) => provider.filter_json(filter)
      None => provider
    }
  })
  @core.open_dataset(sources)
}

///|
fn run(
  options : @core.CommandOptions,
  scope : @nodeio.FileScope,
) -> Unit raise @core.McapError {
  if options.command == "help" {
    println(
      "diff LEFT RIGHT [--json-payloads --bucket-bytes N --aux-bytes N --max-details N --detail-bytes N]; exit 0 equal, 1 different, 2 invalid",
    )
    println(
      "aggregate INPUT... [--value /number --group /field --window NS --origin NS --no-topic-group --max-groups N --group-bytes N] [query filters]",
    )
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
  if options.command == "aggregate" {
    let dataset = query_dataset(options, scope)
    text_output(
      options.output,
      @core.aggregate_json(
        dataset.cursor(options.query),
        dataset.channels(),
        options=options.aggregate,
      ).stringify(),
    )
    return
  }
  if options.command == "diff" {
    let report = @core.diff_sources(
      scope.open(options.inputs[0]),
      scope.open(options.inputs[1]),
      options=options.diff,
    )
    text_output(options.output, report.stringify())
    if report is Object(fields) && fields["equal"] is False {
      set_exit_code(1)
    }
    return
  }
  if ["job-inspect", "job-clean"].contains(options.command) {
    let job = @nodeio.open_merge_job(options.inputs[0], resume_name=true)
    {
      errdefer (job.close() catch { _ => () })

      if options.command == "job-clean" {
        job.clean()
        println("{\"cleaned\":true}")
      } else {
        text_output(options.output, job.inspect())
        job.close()
      }
    }
    return
  }
  if options.command == "external-merge" && options.job_dir != "" {
    let job = @nodeio.open_merge_job(
      options.job_dir,
      resume_name=options.resume_job,
    )
    {
      errdefer (job.close() catch { _ => () })

      let checkpoint = job.checkpoint()
      let sources = options.inputs.map(path => scope.open(path))
      let transaction = job.output(options.output)
      {
        errdefer transaction.abort()

        let report = @core.resumable_merge(
          sources,
          job.storage(),
          transaction.sink(),
          @nodeio.source_sha256,
          bytes => job.save(bytes),
          checkpoint?,
          options=options.external,
        )
        transaction.publish()
        let json = @core.external_report_json(report)
        job.complete(json)
        println(json.stringify())
      }
      job.close()
    }
    return
  }
  if options.content is Some(filter) &&
    ["query", "index-query", "dataset-query"].contains(options.command) {
    let sources = Array::makei(options.inputs.length(), i => {
      let provider = if !options.indexes.is_empty() {
        indexed_reader(options.inputs[i], options.indexes[i], options, scope).query_source()
      } else {
        let reader = @core.open_reader(
          scope.open(options.inputs[i]),
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
    let dataset = @core.open_dataset(sources)
    let cursor = dataset.cursor(options.query)
    if options.format == "ndjson" {
      let transaction = if options.output == "" {
        None
      } else {
        Some(@nodeio.output_transaction(options.output))
      }
      errdefer {
        cursor.cancel()
        if transaction is Some(value) {
          value.abort()
        }
      }

      while cursor.next_record() is Some(record) {
        let text = @core.json_query_record(record, dataset.channels(), filter).stringify() +
          "\n"
        match transaction {
          Some(value) => value.sink().write(@utf8.encode(text[:]))
          None => stdout_text(text)
        }
      }
      if transaction is Some(value) {
        value.publish()
      }
    } else {
      let records = cursor.next_page(options.query.limit.max(1))
      let report : Json = {
        "messages": records.map(record => {
          @core.json_query_record(record, dataset.channels(), filter)
        }),
        "source_count": sources.length(),
      }
      text_output(options.output, report.stringify())
    }
    return
  }
  if options.command == "query" {
    let reader = @core.open_reader(
      scope.open(options.inputs[0]),
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
            @core.new_write_sink(bytes => {
              stdout_text(
                @utf8.decode(bytes[:]) catch {
                  _ => raise @core.io_error("invalid NDJSON UTF-8")
                },
              )
            }),
          ),
        )
      } else {
        let transaction = @nodeio.output_transaction(options.output)
        errdefer transaction.abort()

        ignore(cursor.write_ndjson(transaction.sink()))
        transaction.publish()
      }
      return
    }
    let messages = cursor.next_page(options.query.limit.max(1))
    if options.format == "csv" {
      text_output(
        options.output,
        @core.messages_csv(messages, reader.channels()),
      )
    } else {
      let report : Json = {
        "messages": @core.messages_json(messages, reader.channels()),
        "diagnostics": @core.read_diagnostics_json(reader.diagnostics()),
      }
      text_output(options.output, report.stringify())
    }
    return
  }
  if ["index-build", "index-rebuild"].contains(options.command) {
    let source = scope.open(options.inputs[0])
    let token = @nodeio.file_identity_token(options.inputs[0])
    let bytes = @core.build_sidecar(
      source,
      @nodeio.source_sha256,
      identity=token,
      index_bytes=options.index_bytes,
    )
    if @nodeio.file_identity_token(options.inputs[0]) != token {
      raise @core.io_error("source identity changed while indexing")
    }
    let reader = @core.open_sidecar(
      source,
      bytes,
      @core.TrustIdentity(token),
      index_bytes=options.index_bytes,
    )
    @nodeio.write_atomic_new(options.output, bytes)
    let report : Json = { "build_content_hashed": true, "index": reader.info() }
    println(report.stringify())
    return
  }
  if ["index-check", "index-query"].contains(options.command) {
    let reader = indexed_reader(
      options.inputs[0],
      options.indexes[0],
      options,
      scope,
    )
    if options.command == "index-check" {
      reader.validate_entries()
      text_output(options.output, reader.info().stringify())
    } else {
      let cursor = reader.cursor(options.query)
      if options.format == "ndjson" {
        if options.output == "" {
          ignore(
            cursor.write_ndjson(
              @core.new_write_sink(bytes => {
                stdout_text(
                  @utf8.decode(bytes[:]) catch {
                    _ => raise @core.io_error("invalid NDJSON UTF-8")
                  },
                )
              }),
            ),
          )
        } else {
          let transaction = @nodeio.output_transaction(options.output)
          errdefer transaction.abort()

          ignore(cursor.write_ndjson(transaction.sink()))
          transaction.publish()
        }
      } else {
        let messages = cursor.next_page(options.query.limit.max(1))
        let report : Json = {
          "messages": @core.messages_json(messages, reader.channels()),
          "index": reader.info(),
        }
        text_output(options.output, report.stringify())
      }
    }
    return
  }
  if options.command == "dataset-query" {
    let sources = Array::makei(options.inputs.length(), i => {
      let path = options.inputs[i]
      if !options.indexes.is_empty() {
        return indexed_reader(path, options.indexes[i], options, scope).query_source()
      }
      let reader = @core.open_reader(
        scope.open(path),
        cache_bytes=options.cache_bytes,
        fallback_bytes=options.max_file_bytes,
      )
      if options.full {
        reader.validate_all()
      }
      reader.query_source()
    })
    let dataset = @core.open_dataset(sources)
    let cursor = dataset.cursor(options.query)
    if options.format == "ndjson" {
      if options.output == "" {
        ignore(
          cursor.write_ndjson(
            @core.new_write_sink(bytes => {
              stdout_text(
                @utf8.decode(bytes[:]) catch {
                  _ => raise @core.io_error("invalid NDJSON UTF-8")
                },
              )
            }),
          ),
        )
      } else {
        let transaction = @nodeio.output_transaction(options.output)
        errdefer transaction.abort()

        ignore(cursor.write_ndjson(transaction.sink()))
        transaction.publish()
      }
    } else {
      let messages = cursor.next_page(options.query.limit.max(1))
      let report : Json = {
        "messages": messages.map(fn(record) {
          @core.dataset_message_json(record, dataset.channels())
        }),
        "source_count": sources.length(),
        "materialized_sources": cursor.progress().materialized_sources,
      }
      text_output(options.output, report.stringify())
    }
    return
  }
  if options.command == "recover-blocks" {
    let source = scope.open(options.inputs[0])
    let transaction = @nodeio.output_transaction(options.output)
    {
      errdefer transaction.abort()

      let report = @core.recover_blocks(source, transaction.sink(), options={
        ..@core.stream_options(),
        compression: options.compression,
        chunk_messages: options.chunk_messages,
      })
      transaction.publish()
      println(@core.block_recovery_json(report).stringify())
    }
    return
  }
  if options.command == "stream-transform" {
    let source = scope.open(options.inputs[0])
    let transaction = @nodeio.output_transaction(options.output)
    {
      errdefer transaction.abort()

      let report = @core.stream_transform(
        source,
        @nodeio.sort_directory(options.temp_dir),
        transaction.sink(),
        options.transform,
        external=options.external,
        content?=options.content,
      )
      transaction.publish()
      println(@core.transform_report_json(report).stringify())
    }
    return
  }
  if options.command == "external-merge" {
    let sources = options.inputs.map(path => scope.open(path))
    let transaction = @nodeio.output_transaction(options.output)
    {
      errdefer (transaction.abort() catch {
        @core.Invalid(reason) =>
          raise @core.io_error("external output cleanup failed: \{reason}")
      })

      let storage = @nodeio.sort_directory(options.temp_dir)
      let report = @core.external_merge(
        sources,
        storage,
        transaction.sink(),
        options=options.external,
      )
      transaction.publish()
      println(@core.external_report_json(report).stringify())
    }
    return
  }
  let bytes = @nodeio.read_file(
    options.inputs[0],
    maximum=options.max_file_bytes,
  )
  if options.command == "validate" {
    let report = @core.diagnose_file(bytes)
    text_output(options.output, @core.diagnosis_json(report).stringify())
    if !report.valid {
      set_exit_code(1)
    }
    return
  }
  if options.command == "reindex" {
    let repair = @core.rebuild_indexes(
      bytes,
      compression=options.compression,
      messages_per_chunk=options.chunk_messages,
    )
    @nodeio.write_atomic_new(options.output, repair.bytes)
    let report : Json = {
      "original": @core.diagnosis_json(repair.original),
      "result": @core.diagnosis_json(repair.result),
    }
    println(report.stringify())
    return
  }
  let log = if options.command == "recover" {
    let report = @core.recover_prefix(bytes)
    let detail : Json = {
      "recovered_messages": report.log.messages.length(),
      "stop_offset": report.stop_offset,
      "complete": report.complete,
      "reason": report.reason,
      "source_diagnosis": @core.diagnosis_json(@core.diagnose_file(bytes)),
    }
    error_exit(detail.stringify(), 0)
    report.log
  } else {
    @core.read_log(bytes)
  }
  if options.command == "inspect" {
    text_output(options.output, @core.inspect_json(log).stringify())
    return
  }
  let result = match options.command {
    "transform" => @core.transform_log(log, options.transform)
    "slice" => @core.slice_log(log, options.query)
    "merge" => {
      let logs = [log]
      let mut loaded = bytes.length()
      for i = 1; i < options.inputs.length(); i = i + 1 {
        let next = @nodeio.read_file(
          options.inputs[i],
          maximum=options.max_file_bytes - loaded,
        )
        loaded += next.length()
        logs.push(@core.read_log(next))
      }
      @core.merge_logs(logs)
    }
    _ => log
  }
  let output = @core.write_indexed(
    result,
    compression=options.compression,
    messages_per_chunk=options.chunk_messages,
  )
  @nodeio.write_atomic_new(options.output, output)
  println(@core.inspect_json(result).stringify())
}

///|
fn main {
  let scope = @nodeio.file_scope(max_open=1024)
  run(@core.parse_command(arguments()), scope) catch {
    @core.Invalid(reason) => {
      let error : Json = { "error": reason }
      error_exit(error.stringify(), 2)
    }
  }
  scope.close() catch {
    @core.Invalid(reason) => error_exit(reason, 2)
  }
}
