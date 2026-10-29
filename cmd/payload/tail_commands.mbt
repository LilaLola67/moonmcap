///|
fn tail_file(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  let config = object(json_file(values[2]), [
    "identity", "mode", "snapshot", "units", "batch_bytes", "topics", "start", "end",
    "decode", "where", "quality",
  ])
  let identity = text(config, "identity")
  let mode = match config.get("mode") {
    None => "file"
    Some(String(s)) => s
    _ => raise @mcap.io_error("tail mode string required")
  }
  let previous = match config.get("snapshot") {
    None => Map::new()
    Some(String(path)) => {
      let parsed = @mcap.json_filter("true", max_bytes=256 * 1024 * 1024).project(
        @nodeio.read_file(path, maximum=256 * 1024 * 1024),
      )
      guard parsed is Object(wrapped) &&
        wrapped["data"] is Object(output) &&
        output.get("state") is Some(Object(state)) else {
        raise @mcap.io_error("tail snapshot output required")
      }
      state
    }
    _ => raise @mcap.io_error("snapshot path required")
  }
  let settings = config.copy()
  settings.remove("snapshot")
  if settings.get("quality") is Some(Object(q)) {
    let q = q.copy()
    q.remove("watermark")
    q.remove("finish")
    settings["quality"] = Json::object(q)
  }
  if !previous.is_empty() &&
    previous.get("settings") != Some(Json::object(settings)) {
    raise @mcap.io_error(
      "tail query/settings changed; start a new identity and snapshot",
    )
  }
  fn saved(key : String) -> Bytes? raise @mcap.McapError {
    match previous.get(key) {
      None | Some(Null) => None
      Some(String(s)) => Some(@mcap.decode_hex(s))
      _ => raise @mcap.io_error("invalid tail state")
    }
  }
  fn integer(key : String, fallback : Int) -> Int raise @mcap.McapError {
    match config.get(key) {
      None => fallback
      Some(Number(_, ..) as n) => {
        let v = @mcap.parse_decimal(n.stringify())
        if v > 2147483647UL {
          raise @mcap.io_error("tail budget exceeds Int")
        }
        v.to_int()
      }
      _ => raise @mcap.io_error("tail integer budget required")
    }
  }
  let batch_bytes = integer("batch_bytes", 64 * 1024 * 1024)
  let (batch, snapshot) = if mode == "file" {
    let tail = @mcap.growing_tail(
      identity,
      @nodeio.source_sha256,
      snapshot?=saved("tail"),
      batch_bytes~,
    )
    let batch = tail.poll(scope.open(values[1]), units=integer("units", 32))
    (Some(batch), tail.snapshot())
  } else if mode == "recording" {
    let tail = @mcap.recording_tail(identity, snapshot?=saved("tail"))
    let manifest = @nodeio.read_file(
      values[1] + "/manifest.bin",
      maximum=8 * 1024 * 1024,
    )
    let batch = tail.poll(
      manifest,
      fn(id) { scope.open(values[1] + "/segment-" + id.to_string() + ".mcap") },
      @nodeio.source_sha256,
      batch_bytes~,
    )
    (batch, tail.snapshot())
  } else {
    raise @mcap.io_error("tail mode must be file or recording")
  }
  let topics = match config.get("topics") {
    None => []
    Some(Array(items)) =>
      items.map(fn(v) {
        match v {
          String(s) => s
          _ => raise @mcap.io_error("topic string required")
        }
      })
    _ => raise @mcap.io_error("topics array required")
  }
  fn time(
    fields : Map[String, Json],
    key : String,
    fallback : UInt64,
  ) -> UInt64 raise @mcap.McapError {
    if fields.contains(key) {
      @mcap.parse_decimal(text(fields, key))
    } else {
      fallback
    }
  }
  let start = time(config, "start", 0UL)
  let end = time(config, "end", 18446744073709551615UL)
  if start > end {
    raise @mcap.io_error("invalid tail query window")
  }
  let condition = config
    .get("where")
    .map(fn(v) { @mcap.json_filter(v.stringify()) })
  let quality_config = config
    .get("quality")
    .map(fn(v) {
      object(v, [
        "rules", "start", "lateness", "pending_limit", "watermark", "finish",
      ])
    })
  let quality = quality_config.map(fn(q) {
    guard q.get("rules") is Some(Array(items)) else {
      raise @mcap.io_error("quality rules array required")
    }
    let rules = items.map(fn(item) {
      let rule = object(item, [
        "topic", "minimum_messages", "max_gap", "check_sequence", "condition", "warning",
      ])
      let minimum = match rule.get("minimum_messages") {
        None => 1
        Some(Number(_, ..) as n) => {
          let v = @mcap.parse_decimal(n.stringify())
          if v > 2147483647UL {
            raise @mcap.io_error("quality minimum exceeds Int")
          }
          v.to_int()
        }
        _ => raise @mcap.io_error("quality minimum must be integer")
      }
      {
        ..@mcap.quality_rule(text(rule, "topic")),
        minimum_messages: minimum,
        max_gap: rule
        .get("max_gap")
        .map(fn(_) { @mcap.parse_decimal(text(rule, "max_gap")) }),
        check_sequence: boolean(rule, "check_sequence", false),
        condition: rule.get("condition").map(fn(v) { v.stringify() }),
        warning: boolean(rule, "warning", false),
      }
    })
    let pending_limit = match q.get("pending_limit") {
      None => 4096
      Some(Number(_, ..) as n) => {
        let v = @mcap.parse_decimal(n.stringify())
        if v > 16384UL {
          raise @mcap.io_error("pending limit exceeds 16384")
        }
        v.to_int()
      }
      _ => raise @mcap.io_error("integer pending limit required")
    }
    @mcap.live_quality(
      rules,
      time(q, "start", 0UL),
      lateness=time(q, "lateness", 0UL),
      pending_limit~,
      snapshot?=saved("quality"),
    )
  })
  let decoded_query = boolean(config, "decode", false) || condition is Some(_)
  let quality_payload_topics = match quality_config {
    None => []
    Some(q) => {
      guard q.get("rules") is Some(Array(items)) else {
        raise @mcap.io_error("quality rules required")
      }
      items.filter_map(fn(item) {
        guard item is Object(rule) else { return None }
        if rule.contains("condition") {
          Some(text(rule, "topic"))
        } else {
          None
        }
      })
    }
  }
  let decoder = @mcap.standard_payload_decoder()
  let parser = @mcap.json_filter("true")
  let rows = []
  if batch is Some(batch) {
    for message in batch.messages {
      guard batch.declarations.channels
        .iter()
        .find_first(fn(c) { c.id == message.channel_id })
        is Some(channel) else {
        raise @mcap.io_error("tail message channel missing")
      }
      let selected = (topics.is_empty() || topics.contains(channel.topic)) &&
        message.log_time >= start &&
        message.log_time <= end
      let decoded = (decoded_query && selected) ||
        quality_payload_topics.contains(channel.topic)
      let data = if decoded {
        decoder.decode_message(message, batch.declarations)
      } else {
        message
      }
      if quality is Some(quality) {
        ignore(quality.feed(channel.topic, data))
      }
      if !selected {
        continue
      }
      if condition is Some(predicate) && !predicate.matches(data.data) {
        continue
      }
      let value = if decoded {
        guard parser.project(data.data) is Object(fields) else {
          raise @mcap.io_error("invalid tail JSON")
        }
        fields["data"]
      } else {
        Json::null()
      }
      rows.push(
        (
          {
            "topic": channel.topic,
            "sequence": message.sequence.to_string(),
            "log_time": message.log_time.to_string(),
            "publish_time": message.publish_time.to_string(),
            "data_hex": @mcap.encode_hex(message.data),
            "decoded": value,
          } : Json),
      )
    }
  }
  if quality is Some(quality) && quality_config is Some(q) {
    if q.contains("watermark") {
      quality.advance(@mcap.parse_decimal(text(q, "watermark")))
    }
    if q.contains("finish") {
      quality.finish(@mcap.parse_decimal(text(q, "finish")))
    }
  }
  let output : Json = {
    "rows": rows,
    "evidence": batch
    .map(fn(b) { b.evidence })
    .unwrap_or(({ "verification": "no-new-committed-segment" } : Json)),
    "quality": quality.map(fn(q) { q.report() }).unwrap_or(Json::null()),
    "state": {
      "tail": @mcap.encode_hex(snapshot),
      "quality": quality
      .map(fn(q) { Json::string(@mcap.encode_hex(q.snapshot())) })
      .unwrap_or(Json::null()),
      "settings": Json::object(settings),
    },
  }
  @nodeio.write_atomic_new(
    values[3],
    @utf8.encode((output.stringify() + "\n")[:]),
  )
  println(({ "output": values[3], "rows": rows.length() } : Json).stringify())
}
