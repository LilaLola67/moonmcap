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
async fn main {
  let sources : Array[@aio.AsyncSource] = []
  let caches : Array[@aio.CachedHttpSource] = []
  let outputs : Array[@nodeio.OutputTransaction] = []
  try {
    let values = args()
    if values.is_empty() || values == ["--help"] {
      println(
        "asyncpaged INPUT... --index MIDX2... [--http-cache EXISTING_PARENT --decode --where JSON --select /field --offset N --limit N --output NEW.ndjson]",
      )
      return
    }
    let clean = []
    let mut cache_dir = ""
    let mut decode = false
    let mut i = 0
    while i < values.length() {
      if values[i] == "--http-cache" {
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
      parsed.inputs.length() != parsed.indexes.length() {
      raise @mcap.io_error(
        "asyncpaged requires one MIDX2 per input and full fingerprint verification",
      )
    }
    async fn open(path : String, number : Int32) -> @aio.AsyncSource {
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
    let decoder = @mcap.standard_payload_decoder()
    let infos = []
    for n = 0; n < parsed.inputs.length(); n = n + 1 {
      let source = open(parsed.inputs[n], n * 2)
      let index = open(parsed.indexes[n], n * 2 + 1)
      let reader = @aio.open_disk_sidecar(
        source,
        index,
        @aio.AsyncVerifyContent(@aio.source_sha256),
        chunk_cache_bytes=parsed.cache_bytes,
        index_bytes=parsed.index_bytes.to_uint64(),
      )
      let provider = reader.query_source()
      let provider = if decode {
        provider.decode_payload(decoder)
      } else {
        provider
      }
      providers.push(
        match parsed.content {
          None => provider
          Some(filter) => provider.filter_json(filter)
        },
      )
      infos.push(reader.info())
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
      "indexes": Json::array(infos),
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
  for source in sources {
    source.cancel()
  }
  for cache in caches {
    cache.close() catch {
      error => error_exit("\{error}")
    }
  }
}
