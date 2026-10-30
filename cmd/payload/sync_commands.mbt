///|
fn sync_clock(value : Json) -> @mcap.SyncClock raise @mcap.McapError {
  match value {
    String("log") => @mcap.SyncLogTime
    String("publish") => @mcap.SyncPublishTime
    Object(_) => {
      let fields = object(value, ["nanoseconds", "seconds", "nanos"])
      if fields.contains("nanoseconds") {
        if fields.length() != 1 {
          raise @mcap.io_error(
            "nanoseconds clock cannot also specify stamp fields",
          )
        }
        @mcap.SyncJsonNanoseconds(text(fields, "nanoseconds"))
      } else {
        @mcap.SyncJsonStamp(text(fields, "seconds"), text(fields, "nanos"))
      }
    }
    _ => raise @mcap.io_error("invalid selected clock")
  }
}

///|
fn sync_file(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  let config = object(json_file(values[2]), [
    "topics", "mode", "reuse", "tolerance", "keep_missing", "paths", "grid", "buffer_bytes",
    "json_bytes", "rows", "input_messages",
  ])
  fn integer(key : String, fallback : Int32) -> Int32 raise @mcap.McapError {
    match config.get(key) {
      None => fallback
      Some(Number(_, ..) as value) => {
        let n = @mcap.parse_decimal(value.stringify())
        if n > 2147483647UL {
          raise @mcap.io_error("integer budget exceeds Int")
        }
        n.to_int()
      }
      _ => raise @mcap.io_error("integer budget required: " + key)
    }
  }
  fn u64(key : String, fallback : UInt64) -> UInt64 raise @mcap.McapError {
    if config.contains(key) {
      @mcap.parse_decimal(text(config, key))
    } else {
      fallback
    }
  }
  fn paths(value : Json) -> Array[String] raise @mcap.McapError {
    guard value is Array(items) else {
      raise @mcap.io_error("paths array required")
    }
    items.map(fn(item) {
      match item {
        String(s) => s
        _ => raise @mcap.io_error("path string required")
      }
    })
  }
  let mode = match config.get("mode").unwrap_or(Json::string("nearest")) {
    String("nearest") => @mcap.SyncNearest
    String("previous") => @mcap.SyncPrevious
    String("next") => @mcap.SyncNext
    String("linear") => @mcap.SyncLinear
    _ => raise @mcap.io_error("invalid synchronization mode")
  }
  let options : @mcap.SyncOptions = {
    ..@mcap.sync_options(),
    mode,
    reuse: if boolean(config, "reuse", true) {
      @mcap.SyncAllowReuse
    } else {
      @mcap.SyncConsume
    },
    tolerance: u64("tolerance", 0UL),
    keep_missing: boolean(config, "keep_missing", true),
    interpolation_paths: paths(config.get("paths").unwrap_or(Json::array([]))),
    buffer_bytes: integer("buffer_bytes", 8 * 1024 * 1024),
    json_bytes: integer("json_bytes", 1024 * 1024),
    output_rows: u64("rows", 1000000UL),
    input_messages: u64("input_messages", 10000000UL),
  }
  guard config.get("topics") is Some(Array(selected)) &&
    !selected.is_empty() &&
    selected.length() <= 32 else {
    raise @mcap.io_error("topics requires 1..32 input objects")
  }
  let topics = selected.map(fn(value) {
    let f = object(value, ["topic", "clock", "paths"])
    let topic = text(f, "topic")
    let clock = sync_clock(f.get("clock").unwrap_or(Json::string("log")))
    let selected_paths = f.get("paths").map(paths)
    (topic, clock, selected_paths)
  })
  let source = scope.open(values[1])
  let limits = { ..@mcap.default_limits(), max_records: 30000000, }
  ignore(@mcap.scan_source(source, @mcap.source_visitor(), limits~))
  let sha256 = @nodeio.source_sha256(source)
  let provider = @mcap.open_reader(source, limits~)
    .query_source()
    .decode_payload(
      @mcap.standard_payload_decoder(options={
        ..@mcap.protobuf_options(),
        message_bytes: options.json_bytes,
      }),
      max_bytes=options.json_bytes,
    )
  let streams : Array[@mcap.MessageStream] = []
  defer (for stream in streams {
    stream.cancel()
  })
  let declarations = provider.declaration_log()
  let inputs = topics.map(fn(spec) {
    let (topic, clock, selected_paths) = spec
    if !declarations.channels.any(fn(c) { c.topic == topic }) {
      raise @mcap.io_error("synchronization topic absent: " + topic)
    }
    let stream = provider.stream(query={
      ..@mcap.all_query(),
      topics: [topic],
      limit: 2147483647,
    })
    streams.push(stream)
    @mcap.sync_input(
      topic,
      stream,
      clock=fn(message) {
        @mcap.message_timestamp(message, clock, max_bytes=options.json_bytes)
      },
      interpolation_paths?=selected_paths,
    )
  })
  let stream = match config.get("grid") {
    None => @mcap.synchronize(inputs, options~)
    Some(value) => {
      let grid = object(value, ["start", "end", "period"])
      @mcap.resample(
        inputs,
        @mcap.parse_decimal(text(grid, "start")),
        @mcap.parse_decimal(text(grid, "end")),
        @mcap.parse_decimal(text(grid, "period")),
        options~,
      )
    }
  }
  defer stream.cancel()
  let parser = @mcap.json_filter("true", max_bytes=options.json_bytes)
  let transaction = @nodeio.output_transaction(values[3])
  try {
    while stream.next() is Some(row) {
      let value = @mcap.sync_row_json(row, payload=fn(message) {
        guard parser.project(message.data) is Object(fields) else {
          raise @mcap.io_error("invalid decoded synchronization value")
        }
        fields["data"]
      })
      transaction.sink().write(@utf8.encode((value.stringify() + "\n")[:]))
    }
    transaction.publish()
  } catch {
    error => {
      transaction.abort() catch {
        _ => ()
      }
      raise error
    }
  }
  println(
    (
      {
        "source_sha256": sha256,
        "output": values[3],
        "progress": stream.info(),
      } : Json).stringify(),
  )
}
