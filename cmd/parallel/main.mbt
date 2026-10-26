///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn worker_path() -> String =
  #| ()=>process.getBuiltinModule('node:path').resolve(process.getBuiltinModule('node:path').dirname(process.argv[1]),'../codecworker/codecworker.js')

///|
extern "js" fn report(text : String) -> Unit =
  #| text=>process.stderr.write(text+'\n')

///|
extern "js" fn error_exit(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
extern "js" fn stdout_text(text : String) -> @js_async.Promise[Unit] =
  #| text=>new Promise((resolve,reject)=>{const error=e=>{process.stdout.off('error',error);reject(e);};process.stdout.once('error',error);process.stdout.write(text,e=>{process.stdout.off('error',error);if(e)reject(e);else resolve();});})

///|
async fn main {
  let sources : Array[@aio.AsyncSource] = []
  let pools : Array[@aio.WorkerPool] = []
  let outputs : Array[@nodeio.OutputTransaction] = []
  try {
    let values = args()
    if values.is_empty() || values == ["--help"] {
      println(
        "parallel INPUT... [--decode --workers N --prefetch N --min-chunk-bytes N --min-payload-bytes N --worker-buffer-bytes N --worker-timeout N --worker-entry FILE] [query flags --output NEW.ndjson]",
      )
      return
    }
    let clean = []
    let seen : Map[String, Bool] = Map([])
    let mut config = @aio.worker_options()
    let mut path = worker_path()
    let mut decode = false
    let mut i = 0
    while i < values.length() {
      let flag = values[i]
      if flag == "--decode" {
        if decode {
          raise @mcap.io_error("duplicate decode flag")
        }
        decode = true
        i += 1
      } else if flag == "--worker-entry" ||
        [
          "--workers", "--prefetch", "--min-chunk-bytes", "--min-payload-bytes",
          "--worker-buffer-bytes", "--worker-timeout",
        ].contains(flag) {
        if seen.contains(flag) || i + 1 >= values.length() {
          raise @mcap.io_error("missing or duplicate worker option")
        }
        seen[flag] = true
        if flag == "--worker-entry" {
          path = values[i + 1]
        } else {
          let n = @mcap.parse_decimal(values[i + 1])
          if n > 2147483647UL {
            raise @mcap.io_error("worker option exceeds Int")
          }
          let number = n.to_int()
          config = match flag {
            "--workers" => { ..config, workers: number, }
            "--prefetch" => { ..config, prefetch: number, }
            "--min-chunk-bytes" => { ..config, min_chunk_bytes: number, }
            "--min-payload-bytes" => { ..config, min_payload_bytes: number, }
            "--worker-buffer-bytes" => { ..config, buffer_bytes: number, }
            _ => { ..config, timeout_ms: number, }
          }
        }
        i += 2
      } else {
        clean.push(flag)
        i += 1
      }
    }
    let parsed = @mcap.parse_command(["dataset-query"] + clean)
    if parsed.full || !parsed.indexes.is_empty() {
      raise @mcap.io_error(
        "parallel uses selective internal Chunk indexes, without sidecar flags",
      )
    }
    let pool = @aio.open_worker_pool(path, options=config)
    pools.push(pool)
    let providers = []
    let readers = []
    for path in parsed.inputs {
      let source = if path.has_prefix("https://") || path.has_prefix("http://") {
        @aio.open_http(path)
      } else {
        @aio.open_file(path)
      }
      sources.push(source)
      let reader = @aio.open_parallel_reader(
        source,
        pool,
        cache_bytes=parsed.cache_bytes,
        fallback_bytes=parsed.max_file_bytes,
      )
      readers.push(reader)
      let provider = reader.query_source()
      let provider = if decode {
        provider.decode_parallel(pool)
      } else {
        provider
      }
      providers.push(
        match parsed.content {
          None => provider
          Some(filter) => provider.filter_json(filter)
        },
      )
    }
    let dataset = @aio.open_dataset(providers)
    let cursor = dataset.cursor(parsed.query)
    let sink = if parsed.output == "" {
      @aio.new_sink(async fn(bytes) {
        stdout_text(
          @utf8.decode(bytes[:]) catch {
            _ => raise @mcap.io_error("invalid NDJSON")
          },
        ).wait()
      })
    } else {
      let output = @nodeio.output_transaction(parsed.output)
      outputs.push(output)
      @aio.new_sink(async fn(bytes) { output.sink().write(bytes) })
    }
    ignore(cursor.write_ndjson(sink, content?=parsed.content))
    for output in outputs {
      output.publish()
    }
    let value : Json = {
      "pool": pool.metrics(),
      "readers": Json::array(readers.map(fn(reader) { reader.info() })),
    }
    report(value.stringify())
  } catch {
    error => {
      for output in outputs {
        output.abort() catch {
          _ => ()
        }
      }
      error_exit(
        match error {
          @mcap.Invalid(reason) => reason
          _ => "\{error}"
        },
      )
    }
  }
  for source in sources {
    source.cancel()
  }
  for pool in pools {
    pool.close() catch {
      error => error_exit("\{error}")
    }
  }
}
