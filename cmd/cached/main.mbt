///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn report_error(message : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
extern "js" fn report_metrics(message : String) -> Unit =
  #| text=>process.stderr.write(text+'\n')

///|
extern "js" fn stdout_text(text : String) -> @js_async.Promise[Unit] =
  #| text=>new Promise((resolve,reject)=>{const error=e=>{process.stdout.off('error',error);reject(e);};process.stdout.once('error',error);process.stdout.write(text,e=>{process.stdout.off('error',error);if(e)reject(e);else resolve();});})

///|
async fn main {
  let opened : Array[@aio.CachedHttpSource] = []
  let transactions : Array[@nodeio.OutputTransaction] = []
  try {
    let values = args()
    if values.is_empty() || values == ["--help"] {
      println(
        "cached URL CACHE_DIRECTORY [--page-bytes N --disk-bytes N --parallel N --prefetch N --timeout N --retries N] [--probe OFFSET LENGTH | query flags --output NEW.ndjson]",
      )
      return
    }
    if values.length() < 2 {
      raise @mcap.io_error("cached requires URL and cache directory")
    }
    let mut config = @aio.http_cache_options()
    let clean = [values[0]]
    let seen : Map[String, Bool] = Map([])
    let probes : Array[(UInt64, Int)] = []
    let mut i = 2
    while i < values.length() {
      let flag = values[i]
      if flag == "--probe" {
        if i + 2 >= values.length() {
          raise @mcap.io_error("probe requires offset and length")
        }
        let length = @mcap.parse_decimal(values[i + 2])
        if length > 16UL * 1024UL * 1024UL {
          raise @mcap.io_error("probe byte budget exceeded")
        }
        if probes.length() >= 16 {
          raise @mcap.io_error("probe count budget exceeded")
        }
        probes.push((@mcap.parse_decimal(values[i + 1]), length.to_int()))
        i += 3
      } else if [
          "--page-bytes", "--disk-bytes", "--parallel", "--prefetch", "--timeout",
          "--retries",
        ].contains(flag) {
        if seen.contains(flag) || i + 1 >= values.length() {
          raise @mcap.io_error("missing or duplicate cache option")
        }
        seen[flag] = true
        let number = @mcap.parse_decimal(values[i + 1])
        if number > 2147483647UL {
          raise @mcap.io_error("cache option exceeds Int")
        }
        let value = number.to_int()
        config = match flag {
          "--page-bytes" => { ..config, page_bytes: value, }
          "--disk-bytes" => { ..config, cache_bytes: value, }
          "--parallel" => { ..config, concurrency: value, }
          "--prefetch" => { ..config, prefetch_pages: value, }
          "--timeout" => { ..config, timeout_ms: value, }
          _ => { ..config, retries: value, }
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
        "cached example uses selective range queries without sidecars",
      )
    }
    let cache = @aio.open_cached_http(values[0], values[1], options=config)
    opened.push(cache)
    if !probes.is_empty() {
      @async.with_task_group(async fn(group) {
        let tasks = probes.map(fn(pair) {
          group.spawn(async fn() { cache.source().read_exact(pair.0, pair.1) })
        })
        for task in tasks {
          println(@mcap.encode_hex(task.wait()))
        }
      })
    } else {
      let provider = @aio.open_reader(
        cache.source(),
        cache_bytes=parsed.cache_bytes,
        fallback_bytes=parsed.max_file_bytes,
      ).query_source()
      let dataset = @aio.open_dataset([
        match parsed.content {
          None => provider
          Some(filter) => provider.filter_json(filter)
        },
      ])
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
        transactions.push(output)
        @aio.new_sink(async fn(bytes) { output.sink().write(bytes) })
      }
      ignore(cursor.write_ndjson(sink, content?=parsed.content))
      for output in transactions {
        output.publish()
      }
    }
    report_metrics(cache.metrics().stringify())
  } catch {
    error => {
      for output in transactions {
        output.abort() catch {
          _ => ()
        }
      }
      report_error("\{error}")
    }
  }
  for cache in opened {
    cache.close() catch {
      error => report_error("\{error}")
    }
  }
}
