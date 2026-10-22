///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn clock_text() -> String =
  #| ()=>process.hrtime.bigint().toString()

///|
extern "js" fn write_line(text : String) -> @js_async.Promise[Unit] =
  #| text=>new Promise((resolve,reject)=>{const error=e=>{process.stdout.off('error',error);reject(e);};process.stdout.once('error',error);process.stdout.write(text,e=>{process.stdout.off('error',error);if(e)reject(e);else resolve();});})

///|
extern "js" fn fail_exit(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
async fn main {
  let scope = @nodeio.file_scope()
  try {
    let values = args()
    if values.is_empty() || values == ["--help"] {
      println(
        "replay INPUT [--rate N/D] [--topic TOPIC --start NS --end NS --limit N --where JSON --select /field --index PATH]",
      )
      return
    }
    let clean = []
    let mut numerator = 1
    let mut denominator = 1
    let mut seen = false
    let mut i = 0
    while i < values.length() {
      if values[i] == "--rate" {
        if seen || i + 1 >= values.length() {
          raise @mcap.io_error("--rate needs one positive N/D ratio")
        }
        seen = true
        let (a, b) = match values[i + 1].split_once("/") {
          Some(pair) => pair
          None => raise @mcap.io_error("rate uses N/D")
        }
        let n = @mcap.parse_decimal(a.to_string())
        let d = @mcap.parse_decimal(b.to_string())
        if n < 1UL || d < 1UL || n > 1000000UL || d > 1000000UL {
          raise @mcap.io_error("rate outside range")
        }
        numerator = n.to_int()
        denominator = d.to_int()
        i += 2
      } else {
        clean.push(values[i])
        i += 1
      }
    }
    let options = @mcap.parse_command(
      [if clean.contains("--index") { "index-query" } else { "query" }] + clean,
    )
    if options.output != "" || options.format != "json" {
      raise @mcap.io_error(
        "replay writes NDJSON to stdout and takes query filters, not --output/--format",
      )
    }
    let source = scope.open(options.inputs[0])
    let provider = if options.indexes.is_empty() {
      let reader = @mcap.open_reader(
        source,
        cache_bytes=options.cache_bytes,
        fallback_bytes=options.max_file_bytes,
      )
      if options.full {
        reader.validate_all()
      }
      reader.query_source()
    } else {
      @mcap.open_sidecar(
        source,
        @nodeio.read_file(options.indexes[0], maximum=options.index_bytes),
        if options.fast_index {
          @mcap.TrustIdentity(@nodeio.file_identity_token(options.inputs[0]))
        } else {
          @mcap.VerifyContent(@nodeio.source_sha256)
        },
        index_bytes=options.index_bytes,
        cache_bytes=options.cache_bytes,
      ).query_source()
    }
    if options.full && !options.indexes.is_empty() {
      ignore(@mcap.scan_source(source, @mcap.source_visitor()))
    }
    let replay = @mcap.replay(
      match options.content {
        None => provider
        Some(filter) => provider.filter_json(filter)
      },
      query=options.query,
      rate_numerator=numerator,
      rate_denominator=denominator,
    )
    let channels = provider.declaration_log().channels
    @aio.playback(
      replay,
      fn() { @mcap.parse_decimal(clock_text()) catch { _ => 0UL } },
      async fn(delay) {
        @async.sleep(((delay + 999999UL) / 1000000UL).to_int())
      },
      async fn(message) {
        let value = match options.content {
          None =>
            @mcap.dataset_message_json({ source_index: 0, message, }, channels)
          Some(filter) =>
            @mcap.json_query_record(
              { source_index: 0, message, },
              channels,
              filter,
            )
        }
        if value is Object(fields) {
          fields["dispatch_clock_ns"] = Json::string(clock_text())
        }
        write_line(value.stringify() + "\n").wait()
      },
    )
  } catch {
    error => fail_exit("\{error}")
  }
  scope.close() catch {
    @mcap.Invalid(reason) => fail_exit(reason)
  }
}
