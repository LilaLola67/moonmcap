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
async fn main {
  let sources : Array[@aio.AsyncSource] = []
  let transactions : Array[@nodeio.OutputTransaction] = []
  try {
    let values = args()
    if values.is_empty() || values == ["--help"] {
      println(
        "dataset INPUT_OR_URL... [--where JSON --select /field --index SIDECAR --offset N --limit N --descending --output NEW.ndjson]",
      )
      return
    }
    let options = @mcap.parse_command(["dataset-query"] + values)
    if options.full {
      raise @mcap.io_error(
        "unified example uses selective queries; use validate for complete source checking",
      )
    }
    let providers : Array[@aio.AsyncQuerySource] = []
    for i = 0; i < options.inputs.length(); i = i + 1 {
      let path = options.inputs[i]
      let remote = path.has_prefix("http://") || path.has_prefix("https://")
      let source = if remote {
        @aio.open_http(path)
      } else {
        @aio.open_file(path)
      }
      sources.push(source)
      let provider = if options.indexes.is_empty() {
        @aio.open_reader(
          source,
          cache_bytes=options.cache_bytes,
          fallback_bytes=options.max_file_bytes,
        ).query_source()
      } else {
        if remote && options.fast_index {
          raise @mcap.io_error(
            "remote sidecars require content hashing in this example",
          )
        }
        let mode = if options.fast_index {
          @aio.AsyncTrustIdentity(@nodeio.file_identity_token(path))
        } else {
          @aio.AsyncVerifyContent(@aio.source_sha256)
        }
        @aio.open_sidecar(
          source,
          @nodeio.read_file(options.indexes[i], maximum=options.index_bytes),
          mode,
          index_bytes=options.index_bytes,
          cache_bytes=options.cache_bytes,
        ).query_source()
      }
      providers.push(
        match options.content {
          None => provider
          Some(filter) => provider.filter_json(filter)
        },
      )
    }
    let dataset = @aio.open_dataset(providers)
    let cursor = dataset.cursor(options.query)
    let sink = if options.output == "" {
      @aio.new_sink(async fn(bytes) {
        stdout_text(
          @utf8.decode(bytes[:]) catch {
            _ => raise @mcap.io_error("invalid NDJSON")
          },
        ).wait()
      })
    } else {
      let transaction = @nodeio.output_transaction(options.output)
      transactions.push(transaction)
      @aio.new_sink(async fn(bytes) { transaction.sink().write(bytes) })
    }
    ignore(cursor.write_ndjson(sink, content?=options.content))
    for transaction in transactions {
      transaction.publish()
    }
  } catch {
    error => {
      for transaction in transactions {
        transaction.abort() catch {
          _ => ()
        }
      }
      error_exit("\{error}")
    }
  }
  for source in sources {
    source.cancel()
  }
}
