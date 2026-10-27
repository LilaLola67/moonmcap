///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn stdout_text(text : String) -> @js_async.Promise[Unit] =
  #| text=>new Promise((resolve,reject)=>{const error=e=>{process.stdout.off('error',error);reject(e);};process.stdout.once('error',error);process.stdout.write(text,e=>{process.stdout.off('error',error);if(e)reject(e);else resolve();});})

///|
extern "js" fn error_exit(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
extern "js" fn report(text : String) -> Unit =
  #| text=>process.stderr.write(text+'\n')

///|
extern "js" fn worker_path() -> String =
  #| ()=>process.getBuiltinModule('node:path').resolve(process.getBuiltinModule('node:path').dirname(process.argv[1]),'../codecworker/codecworker.js')

///|
async fn main {
  let sources : Array[@aio.AsyncSource] = []
  let caches : Array[@aio.CachedHttpSource] = []
  let pools : Array[@aio.WorkerPool] = []
  let outputs : Array[@nodeio.OutputTransaction] = []
  try {
    let values = args()
    if values.is_empty() || values == ["--help"] {
      println(
        "query INPUT... [--index MIDX2... --field-index MFLD1... --workers N --worker-entry FILE] [--http-cache EXISTING_PARENT --decode --where JSON --select /field --offset N --limit N --output NEW.ndjson]",
      )
      return
    }
    let clean = []
    let mut cache_dir = ""
    let mut decode = false
    let fields = []
    let mut workers = 0
    let mut entry = worker_path()
    let seen : Map[String, Bool] = Map([])
    let mut i = 0
    while i < values.length() {
      if ["--field-index", "--workers", "--worker-entry"].contains(values[i]) {
        let flag = values[i]
        if i + 1 >= values.length() ||
          (flag != "--field-index" && seen.contains(flag)) {
          raise @mcap.io_error("missing or duplicate pipeline option")
        }
        seen[flag] = true
        match flag {
          "--field-index" => fields.push(values[i + 1])
          "--worker-entry" => entry = values[i + 1]
          _ => {
            let n = @mcap.parse_decimal(values[i + 1])
            if n > 8UL {
              raise @mcap.io_error("workers must be 0..8")
            }
            workers = n.to_int()
          }
        }
        i += 2
      } else if values[i] == "--http-cache" {
        if cache_dir != "" || i + 1 >= values.length() {
          raise @mcap.io_error("missing or duplicate HTTP cache directory")
        }
        cache_dir = values[i + 1]
        i += 2
      } else if values[i] == "--decode" {
        if decode {
          raise @mcap.io_error("duplicate decode option")
        }
        decode = true
        i += 1
      } else {
        clean.push(values[i])
        i += 1
      }
    }
    let parsed = @mcap.parse_command(["dataset-query"] + clean)
    if parsed.full ||
      parsed.fast_index ||
      (
        !parsed.indexes.is_empty() &&
        parsed.inputs.length() != parsed.indexes.length()
      ) ||
      (!fields.is_empty() && parsed.inputs.length() != fields.length()) {
      raise @mcap.io_error(
        "query needs one index per input when supplied, with full content verification",
      )
    }
    async fn open(path : String, number : Int) -> @aio.AsyncSource {
      let remote = path.has_prefix("https://") || path.has_prefix("http://")
      let source = if remote && cache_dir != "" {
        let cache = @aio.open_cached_http(path, cache_dir + "/item-\{number}")
        caches.push(cache)
        cache.source()
      } else if remote {
        @aio.open_http(path)
      } else {
        @aio.open_file(path)
      }
      sources.push(source)
      source
    }
    let providers = []
    let plans = []
    let pool = if workers == 0 {
      None
    } else {
      let pool = @aio.open_worker_pool(entry, options={
        ..@aio.worker_options(),
        workers,
      })
      pools.push(pool)
      Some(pool)
    }
    for n = 0; n < parsed.inputs.length(); n = n + 1 {
      let source = open(parsed.inputs[n], n * 3)
      let index = if parsed.indexes.is_empty() {
        None
      } else {
        Some(open(parsed.indexes[n], n * 3 + 1))
      }
      let fields = if fields.is_empty() {
        None
      } else {
        Some(open(fields[n], n * 3 + 2))
      }
      let plan = @aio.plan_query(
        source,
        index?,
        fields?,
        pool?,
        filter?=parsed.content,
        decode~,
        cache_bytes=parsed.cache_bytes,
        fallback_bytes=parsed.max_file_bytes,
      )
      plans.push(plan)
      providers.push(plan.query_source())
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
      "plans": Json::array(plans.map(fn(p) { p.info() })),
      "pools": Json::array(pools.map(fn(p) { p.metrics() })),
      "http_caches": Json::array(caches.map(fn(c) { c.metrics() })),
    }
    report(value.stringify())
  } catch {
    error => {
      for output in outputs {
        output.abort() catch {
          _ => ()
        }
      }
      error_exit("\{error}")
    }
  }
  for pool in pools {
    pool.close() catch {
      error => error_exit("\{error}")
    }
  }
  for source in sources {
    source.cancel()
  }
  for cache in caches {
    cache.close() catch {
      error => error_exit("\{error}")
    }
  }
}
