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
        "join INPUT... --left TOPIC --right TOPIC [--mode nearest|asof --tolerance NS --missing keep|drop --join-buffer-bytes N] [--index MIDX2... --field-index MFLD1... --workers N --worker-entry FILE] [--http-cache EXISTING_PARENT --decode --where JSON --select /field --offset N --limit N --output NEW.ndjson]",
      )
      return
    }
    let values_clean = []
    let mut left_topic = ""
    let mut right_topic = ""
    let mut join_config = @mcap.join_options()
    let join_seen : Map[String, Bool] = Map([])
    let mut j = 0
    while j < values.length() {
      let flag = values[j]
      if [
          "--left", "--right", "--mode", "--tolerance", "--missing", "--join-buffer-bytes",
        ].contains(flag) {
        if j + 1 >= values.length() || join_seen.contains(flag) {
          raise @mcap.io_error("missing or duplicate join option")
        }
        join_seen[flag] = true
        let value = values[j + 1]
        match flag {
          "--left" => left_topic = value
          "--right" => right_topic = value
          "--mode" =>
            join_config = {
              ..join_config,
              mode: match value {
                "nearest" => @mcap.JoinNearest
                "asof" => @mcap.JoinAsOf
                _ => raise @mcap.io_error("mode must be nearest or asof")
              },
            }
          "--missing" =>
            join_config = {
              ..join_config,
              missing: match value {
                "keep" => @mcap.JoinKeep
                "drop" => @mcap.JoinDrop
                _ => raise @mcap.io_error("missing must be keep or drop")
              },
            }
          "--tolerance" =>
            join_config = {
              ..join_config,
              tolerance: @mcap.parse_decimal(value),
            }
          _ => {
            let n = @mcap.parse_decimal(value)
            if n > 2147483647UL {
              raise @mcap.io_error("join byte budget overflow")
            }
            join_config = { ..join_config, buffer_bytes: n.to_int(), }
          }
        }
        j += 2
      } else {
        values_clean.push(flag)
        j += 1
      }
    }
    if left_topic == "" || right_topic == "" || left_topic == right_topic {
      raise @mcap.io_error("join requires distinct --left and --right topics")
    }
    let values = values_clean
    let clean = []
    let mut cache_dir = ""
    let mut decode = false
    let mut projected = false
    let fields = []
    let mut workers = 0
    let mut entry = worker_path()
    let seen : Map[String, Bool] = Map([])
    let mut i = 0
    while i < values.length() {
      if values[i] == "--project-decode" {
        if projected {
          raise @mcap.io_error("duplicate projection flag")
        }
        projected = true
        i += 1
      } else if ["--field-index", "--workers", "--worker-entry"].contains(
          values[i],
        ) {
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
    if parsed.query.descending || !parsed.query.topics.is_empty() {
      raise @mcap.io_error(
        "join requires ascending input; use --left/--right for topics",
      )
    }
    let selection = if projected {
      Some({
        ..@mcap.decode_selection(),
        paths: parsed.content.map(fn(f) { f.selected_paths() }).unwrap_or([]),
      })
    } else {
      None
    }
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
        selection?,
        cache_bytes=parsed.cache_bytes,
        fallback_bytes=parsed.max_file_bytes,
      )
      plans.push(plan)
      providers.push(plan.query_source())
    }
    let dataset = @aio.open_dataset(providers)
    let base = { ..parsed.query, offset: 0, limit: 2147483647, }
    let left = dataset.cursor({ ..base, topics: [left_topic], })
    let tolerance = join_config.tolerance
    let start = if base.start_time < tolerance {
      0UL
    } else {
      base.start_time - tolerance
    }
    let end = if join_config.mode == @mcap.JoinAsOf {
      base.end_time
    } else if tolerance > 18446744073709551615UL - base.end_time {
      18446744073709551615UL
    } else {
      base.end_time + tolerance
    }
    let right = dataset.cursor({
      ..base,
      topics: [right_topic],
      start_time: start,
      end_time: end,
    })
    let join = @aio.temporal_join(
      @aio.async_message_stream(
        async fn() { left.next().map(fn(row) { row.message }) },
        fn() { left.cancel() },
      ),
      @aio.async_message_stream(
        async fn() { right.next().map(fn(row) { row.message }) },
        fn() { right.cancel() },
      ),
      options=join_config,
    )
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
    let channels = dataset.channels()
    let content = if decode || projected {
      Some(parsed.content.unwrap_or(@mcap.json_filter("true")))
    } else {
      parsed.content
    }
    let message_json = fn(
      message : @mcap.Message,
    ) -> Json raise @mcap.McapError {
      let channel = channels
        .iter()
        .find_first(fn(c) { c.id == message.channel_id })
        .unwrap()
      {
        "topic": channel.topic,
        "channel_id": message.channel_id,
        "sequence": message.sequence.to_string(),
        "log_time": message.log_time.to_string(),
        "publish_time": message.publish_time.to_string(),
        "payload": match content {
          Some(f) => f.project(message.data)
          None => Json::string(@mcap.encode_hex(message.data))
        },
      }
    }
    let mut skipped = 0
    let mut emitted = 0
    try {
      while emitted < parsed.query.limit {
        guard join.next() is Some(row) else { break }
        if skipped < parsed.query.offset {
          skipped += 1
          continue
        }
        let value : Json = {
          "left": message_json(row.left),
          "right": match row.right {
            None => Json::null()
            Some(m) => message_json(m)
          },
          "distance": match row.distance {
            None => Json::null()
            Some(d) => Json::string(d.to_string())
          },
          "right_is_earlier": row.right_is_earlier,
        }
        sink.write(@utf8.encode((value.stringify() + "\n")[:]))
        emitted += 1
      }
    } catch {
      error => {
        join.cancel()
        raise error
      }
    }
    join.cancel()
    for output in outputs {
      output.publish()
    }
    let value : Json = {
      "join": join.info(),
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
      error_exit(
        match error {
          @mcap.Invalid(reason) => reason
          _ => "\{error}"
        },
      )
    }
  }
  for pool in pools {
    pool.close() catch {
      error =>
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
  for cache in caches {
    cache.close() catch {
      error =>
        error_exit(
          match error {
            @mcap.Invalid(reason) => reason
            _ => "\{error}"
          },
        )
    }
  }
}
