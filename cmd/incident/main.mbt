///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn exit_status(code : Int) -> Unit =
  #| code=>{process.exitCode=code;}

///|
extern "js" fn error_text(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
fn object(
  value : Json,
  allowed : Array[String],
) -> Map[String, Json] raise @mcap.McapError {
  guard value is Object(fields) else {
    raise @mcap.io_error("configuration must be an object")
  }
  for key in fields.keys() {
    if !allowed.contains(key) {
      raise @mcap.io_error("unknown configuration key: " + key)
    }
  }
  fields
}

///|
fn text(
  fields : Map[String, Json],
  key : String,
) -> String raise @mcap.McapError {
  match fields.get(key) {
    Some(String(value)) => value
    _ => raise @mcap.io_error("expected string: " + key)
  }
}

///|
fn nanoseconds(
  fields : Map[String, Json],
  key : String,
  fallback : UInt64,
) -> UInt64 raise @mcap.McapError {
  if !fields.contains(key) {
    return fallback
  }
  @mcap.parse_decimal(text(fields, key))
}

///|
fn count(
  fields : Map[String, Json],
  key : String,
  fallback : Int,
) -> Int raise @mcap.McapError {
  match fields.get(key) {
    None => fallback
    Some(Number(_, ..) as value) => {
      let number = @mcap.parse_decimal(value.stringify())
      if number > 2147483647UL {
        raise @mcap.io_error("integer configuration overflow")
      }
      number.to_int()
    }
    _ => raise @mcap.io_error("expected integer: " + key)
  }
}

///|
fn boolean(
  fields : Map[String, Json],
  key : String,
  fallback : Bool,
) -> Bool raise @mcap.McapError {
  match fields.get(key) {
    None => fallback
    Some(True) => true
    Some(False) => false
    _ => raise @mcap.io_error("expected boolean: " + key)
  }
}

///|
fn array(
  fields : Map[String, Json],
  key : String,
) -> Array[Json] raise @mcap.McapError {
  match fields.get(key) {
    None => []
    Some(Array(values)) => values
    _ => raise @mcap.io_error("expected array: " + key)
  }
}

///|
fn context_rules(
  config : Map[String, Json],
) -> Array[@mcap.ContextRule] raise @mcap.McapError {
  array(config, "context").map(fn(value) {
    let fields = object(value, ["topic", "depth", "max_age", "required"])
    {
      topic: text(fields, "topic"),
      depth: count(fields, "depth", 1),
      max_age: nanoseconds(fields, "max_age", 18446744073709551615UL),
      required: boolean(fields, "required", true),
    }
  })
}

///|
fn topics(config : Map[String, Json]) -> Array[String] raise @mcap.McapError {
  array(config, "topics").map(fn(value) {
    match value {
      String(value) if value != "" => value
      _ => raise @mcap.io_error("topics require nonempty strings")
    }
  })
}

///|
fn rule(config : Map[String, Json]) -> @mcap.EventRule raise @mcap.McapError {
  let fields = object(config.get("rule").unwrap_or(Json::null()), [
    "topic", "enter", "leave", "duration", "max_gap", "cooldown", "pre", "post",
  ])
  let enter = fields.get("enter").unwrap_or(Json::null()).stringify()
  {
    topic: text(fields, "topic"),
    enter,
    leave: fields.get("leave").map(fn(value) { value.stringify() }),
    duration: nanoseconds(fields, "duration", 0UL),
    max_gap: nanoseconds(fields, "max_gap", 1000000000UL),
    cooldown: nanoseconds(fields, "cooldown", 0UL),
    pre: nanoseconds(fields, "pre", 0UL),
    post: nanoseconds(fields, "post", 0UL),
  }
}

///|
fn load_config(path : String) -> Json raise @mcap.McapError {
  // Reuse the bounded strict JSON parser (including duplicate-key rejection).
  let wrapper = @mcap.json_filter("true").project(
    @nodeio.read_file(path, maximum=1024 * 1024),
  )
  match wrapper {
    Object(fields) => fields["data"]
    _ => raise @mcap.io_error("invalid config")
  }
}

///|
fn emit_clip(
  prefix : String,
  ordinal : Int,
  log : @mcap.Log,
  manifest : Json,
) -> Json raise @mcap.McapError {
  let path = prefix + "-" + ordinal.to_string() + ".mcap"
  let log = {
    ..log,
    metadata: log.metadata +
    [
      {
        name: "moonmcap.incident",
        entries: [("manifest", manifest.stringify())],
      },
    ],
  }
  @nodeio.write_atomic_new(
    path,
    @mcap.write_indexed(log, compression=@mcap.Zstd),
  )
  { "output": path, "manifest": manifest }
}

///|
fn run(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  if values.is_empty() || values == ["--help"] {
    println(
      "incident quality|events|clip|capture|clock INPUT CONFIG.json [NEW_OUTPUT_OR_PREFIX]; see docs/incident-workflows.md",
    )
    return
  }
  if values.length() < 3 ||
    values.length() > 4 ||
    !["quality", "events", "clip", "capture", "clock"].contains(values[0]) {
    raise @mcap.io_error("invalid incident command")
  }
  let mode = values[0]
  if ["clip", "capture", "clock"].contains(mode) && values.length() != 4 {
    raise @mcap.io_error("this command requires a new output path or prefix")
  }
  if mode == "quality" && values.length() != 3 {
    raise @mcap.io_error("quality report is stdout only")
  }
  let configuration = load_config(values[2])
  let allowed = match mode {
    "quality" => ["start", "end", "rules", "pairs", "detail_limit", "decode"]
    "events" =>
      [
        "rule", "start", "end", "event_limit", "merge_gap", "topics", "context",
        "clip_bytes", "clip_messages", "decode",
      ]
    "clip" =>
      ["start", "end", "topics", "context", "clip_bytes", "clip_messages"]
    "capture" =>
      [
        "rule", "buffer_bytes", "buffer_messages", "clip_bytes", "clip_messages",
        "event_limit", "decode",
      ]
    _ =>
      [
        "anchors", "probes", "extrapolate", "same_publish_clock", "max_file_bytes",
      ]
  }
  let config = object(configuration, allowed)
  let source = scope.open(values[1])
  let _ = @mcap.scan_source(source, @mcap.source_visitor())
  let fingerprint = @nodeio.source_sha256(source)
  let output = if values.length() == 4 { values[3] } else { "" }
  if mode == "clock" {
    fn anchors(key : String) -> Array[@mcap.ClockAnchor] raise @mcap.McapError {
      array(config, key).map(fn(value) {
        let a = object(value, ["source", "target"])
        {
          source: @mcap.parse_decimal(text(a, "source")),
          target: @mcap.parse_decimal(text(a, "target")),
        }
      })
    }
    let mapping = @mcap.clock_map(
      anchors("anchors"),
      extrapolate=boolean(config, "extrapolate", false),
    )
    let maximum = count(config, "max_file_bytes", 64 * 1024 * 1024)
    if maximum < 1 {
      raise @mcap.io_error("max_file_bytes must be positive")
    }
    if source.size() > maximum.to_uint64() {
      raise @mcap.io_error("clock input exceeds explicit file budget")
    }
    let log = @mcap.read_log(source.read_exact(0UL, source.size().to_int()))
    let corrected = @mcap.calibrate_log(
      log,
      mapping,
      same_publish_clock=boolean(config, "same_publish_clock", false),
    )
    // Evaluate probes before publishing: invalid probes cannot leave a success file.
    let residuals = mapping.residuals(anchors("probes"))
    @nodeio.write_atomic_new(
      output,
      @mcap.write_indexed(corrected, compression=@mcap.Zstd),
    )
    println(
      (
        {
          "source_sha256": fingerprint,
          "output": output,
          "mapping": mapping.json(),
          "residuals": residuals,
        } : Json).stringify(),
    )
    return
  }
  let raw = @mcap.open_reader(source).query_source()
  let decoded = if boolean(config, "decode", false) {
    raw.decode_payload(
      @mcap.standard_payload_decoder(),
      max_bytes=16 * 1024 * 1024,
    )
  } else {
    raw
  }
  if mode == "quality" {
    let start = @mcap.parse_decimal(text(config, "start"))
    let end = @mcap.parse_decimal(text(config, "end"))
    let rules : Array[@mcap.QualityRule] = array(config, "rules").map(fn(
      value,
    ) {
      let f = object(value, [
        "topic", "minimum_messages", "max_gap", "check_sequence", "condition", "warning",
      ])
      {
        topic: text(f, "topic"),
        minimum_messages: count(f, "minimum_messages", 1),
        max_gap: if f.contains("max_gap") {
          Some(nanoseconds(f, "max_gap", 0UL))
        } else {
          None
        },
        check_sequence: boolean(f, "check_sequence", false),
        condition: f.get("condition").map(fn(value) { value.stringify() }),
        warning: boolean(f, "warning", false),
      }
    })
    let quality = @mcap.check_quality(
      decoded,
      rules,
      start,
      end,
      detail_limit=count(config, "detail_limit", 64),
    )
    let pairs : Array[Json] = array(config, "pairs").map(fn(value) {
      let f = object(value, [
        "left", "right", "tolerance", "minimum_basis_points",
      ])
      let left = text(f, "left")
      let right = text(f, "right")
      if left == right ||
        !raw.declaration_log().channels.any(fn(c) { c.topic == left }) ||
        !raw.declaration_log().channels.any(fn(c) { c.topic == right }) {
        raise @mcap.io_error("pair requires existing distinct topics")
      }
      let l = raw.stream(query={
        ..@mcap.all_query(),
        topics: [left],
        start_time: start,
        end_time: end,
      })
      defer l.cancel()
      let r = raw.stream(query={
        ..@mcap.all_query(),
        topics: [right],
        start_time: start,
        end_time: end,
      })
      @mcap.check_match_quality(
        l,
        r,
        nanoseconds(f, "tolerance", 0UL),
        count(f, "minimum_basis_points", 10000),
      )
    })
    let failed_pair = pairs.any(fn(pair) {
      match pair {
        Object(f) => f.get("status") == Some(Json::string("failed"))
        _ => true
      }
    })
    let status = match quality {
      Object(f) => f["status"]
      _ => Json::string("failed")
    }
    println(
      (
        { "source_sha256": fingerprint, "quality": quality, "pairs": pairs } :
        Json).stringify(),
    )
    if status == Json::string("failed") || failed_pair {
      exit_status(1)
    }
    return
  }
  if mode == "clip" {
    let clip = @mcap.extract_context_clip(
      raw,
      @mcap.parse_decimal(text(config, "start")),
      @mcap.parse_decimal(text(config, "end")),
      topics=topics(config),
      context=context_rules(config),
      message_limit=count(config, "clip_messages", 100000),
      byte_limit=count(config, "clip_bytes", 64 * 1024 * 1024),
    )
    @nodeio.write_atomic_new(
      output,
      @mcap.write_indexed(clip.log, compression=@mcap.Zstd),
    )
    println(
      (
        {
          "source_sha256": fingerprint,
          "manifest": clip.report,
          "output": output,
        } : Json).stringify(),
    )
    return
  }
  let rule = rule(config)
  if mode == "events" {
    let events = @mcap.detect_events(
      decoded,
      rule,
      start=nanoseconds(config, "start", 0UL),
      end=nanoseconds(config, "end", 18446744073709551615UL),
      event_limit=count(config, "event_limit", 1024),
    )
    let windows = @mcap.event_windows(
      events,
      rule.pre,
      rule.post,
      merge_gap=nanoseconds(config, "merge_gap", 0UL),
    )
    let outputs : Array[Json] = []
    for i = 0; i < windows.length(); i = i + 1 {
      let window = windows[i]
      let manifest : Json = {
        "start": window.start.to_string(),
        "end": window.end.to_string(),
        "events": window.events.map(@mcap.event_span_json),
      }
      if output != "" {
        let clip = @mcap.extract_context_clip(
          raw,
          window.start,
          window.end,
          topics=topics(config),
          context=context_rules(config),
          message_limit=count(config, "clip_messages", 100000),
          byte_limit=count(config, "clip_bytes", 64 * 1024 * 1024),
        )
        outputs.push(
          emit_clip(output, i, clip.log, {
            "source_sha256": fingerprint,
            "event": manifest,
            "context": clip.report,
          }),
        )
      } else {
        outputs.push(manifest)
      }
    }
    println(
      (
        {
          "source_sha256": fingerprint,
          "events": events.map(@mcap.event_span_json),
          "windows": outputs,
        } : Json).stringify(),
    )
    return
  }
  let detector = @mcap.event_detector(rule)
  let captures : Array[Json] = []
  let recorder = @mcap.trigger_recorder(
    raw.declaration_log(),
    fn(log, manifest) {
      captures.push(
        emit_clip(output, captures.length(), log, {
          "source_sha256": fingerprint,
          "capture": manifest,
        }),
      )
    },
    options={
      pre: rule.pre,
      post: rule.post,
      buffer_bytes: count(config, "buffer_bytes", 16 * 1024 * 1024),
      buffer_messages: count(config, "buffer_messages", 10000),
      clip_bytes: count(config, "clip_bytes", 64 * 1024 * 1024),
      clip_messages: count(config, "clip_messages", 100000),
      event_limit: count(config, "event_limit", 1024),
    },
  )
  defer recorder.cancel()
  let channels = raw.declaration_log().channels
  if !channels.any(fn(c) { c.topic == rule.topic }) {
    raise @mcap.io_error("capture event topic absent")
  }
  let stream = raw.stream()
  defer stream.cancel()
  let decoder = @mcap.standard_payload_decoder()
  // This command replays a file into the same incremental API used by a live
  // producer. It does not pretend to subscribe to a ROS network.
  while stream.next() is Some(message) {
    recorder.feed(message)
    if channels.any(fn(c) {
        c.id == message.channel_id && c.topic == rule.topic
      }) {
      let evaluated = if boolean(config, "decode", false) {
        // Decode one message through a source-local adapter without changing output bytes.
        let singleton = @mcap.build_query_index({
            ..raw.declaration_log(),
            messages: [message],
          })
          .query_source()
          .decode_payload(decoder, max_bytes=16 * 1024 * 1024)
          .stream()
        defer singleton.cancel()
        singleton.next().unwrap()
      } else {
        message
      }
      for signal in detector.feed(evaluated) {
        if signal is @mcap.EventStarted(_, _) {
          recorder.trigger("rule")
        }
      }
    }
  }
  recorder.finish()
  println(
    (
      {
        "source_sha256": fingerprint,
        "captures": captures,
        "progress": recorder.progress(),
        "input_mode": "file replay into incremental capture API",
      } : Json).stringify(),
  )
}

///|
fn main {
  let scope = @nodeio.file_scope()
  run(args(), scope) catch {
    @mcap.Invalid(reason) => error_text(reason)
  }
  scope.close() catch {
    @mcap.Invalid(reason) => error_text(reason)
  }
}
