///|
fn normalized_rules(
  config : Map[String, Json],
) -> Array[@mcap.NormalizedField] raise @mcap.McapError {
  guard config.get("fields") is Some(Array(items)) else {
    raise @mcap.io_error("normalization fields required")
  }
  items.map(fn(item) {
    let f = object(item, [
      "source", "target", "default", "required", "conversion", "bits", "scale", "offset",
    ])
    let kind = match f.get("conversion") {
      None => "keep"
      Some(String(s)) => s
      _ => raise @mcap.io_error("conversion string expected")
    }
    fn number(key : String, fallback : Double) -> Double raise @mcap.McapError {
      match f.get(key) {
        None => fallback
        Some(Number(n, ..)) => n
        _ => raise @mcap.io_error("number expected: " + key)
      }
    }
    let conversion = match kind {
      "keep" => @mcap.KeepValue
      "text" => @mcap.TextValue
      "boolean" => @mcap.BooleanValue
      "scale" => @mcap.ScaleValue(number("scale", 1.0), number("offset", 0.0))
      "signed" | "unsigned" => {
        let width = number("bits", 64.0)
        if ![8.0, 16.0, 32.0, 64.0].contains(width) {
          raise @mcap.io_error("invalid integer width")
        }
        if kind == "signed" {
          @mcap.SignedValue(width.to_int())
        } else {
          @mcap.UnsignedValue(width.to_int())
        }
      }
      _ => raise @mcap.io_error("unknown normalization conversion")
    }
    if kind != "scale" && (f.contains("scale") || f.contains("offset")) {
      raise @mcap.io_error("scale/offset require scale conversion")
    }
    if kind != "signed" && kind != "unsigned" && f.contains("bits") {
      raise @mcap.io_error("bits require integer conversion")
    }
    {
      source: text(f, "source"),
      target: text(f, "target"),
      default_value: f.get("default"),
      required: boolean(f, "required", true),
      conversion,
    }
  })
}

///|
fn normalize_file(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  let config = object(json_file(values[2]), [
    "fields", "topics", "where", "start", "end", "limit",
  ])
  let fields = normalized_rules(config)
  let normalizer = @mcap.payload_normalizer(fields)
  let topics = match config.get("topics") {
    None => []
    Some(Array(items)) =>
      items.map(fn(item) {
        match item {
          String(s) if s != "" => s
          _ => raise @mcap.io_error("topic string required")
        }
      })
    _ => raise @mcap.io_error("topics array expected")
  }
  fn time(key : String, fallback : UInt64) -> UInt64 raise @mcap.McapError {
    if config.contains(key) {
      @mcap.parse_decimal(text(config, key))
    } else {
      fallback
    }
  }
  let limit = match config.get("limit") {
    None => 100000UL
    Some(Number(_, ..) as n) => @mcap.parse_decimal(n.stringify())
    _ => raise @mcap.io_error("limit integer required")
  }
  let filter = @mcap.json_filter(
    config.get("where").unwrap_or(Json::boolean(true)).stringify(),
  )
  let source = scope.open(values[1])
  let limits = { ..@mcap.default_limits(), max_records: 30000000, }
  ignore(@mcap.scan_source(source, @mcap.source_visitor(), limits~))
  let sha256 = @nodeio.source_sha256(source)
  let provider = @mcap.open_reader(source, limits~)
    .query_source()
    .decode_payload(@mcap.standard_payload_decoder())
  let declarations = provider.declaration_log()
  for topic in topics {
    if !declarations.channels.any(fn(c) { c.topic == topic }) {
      raise @mcap.io_error("normalization topic absent")
    }
  }
  let stream = provider.stream(query={
    ..@mcap.all_query(),
    topics,
    start_time: time("start", 0UL),
    end_time: time("end", 18446744073709551615UL),
  })
  defer stream.cancel()
  let transaction = @nodeio.output_transaction(values[3])
  let mut rows = 0UL
  try {
    while rows < limit && stream.next() is Some(message) {
      let wrapper = @mcap.json_filter("true").project(message.data)
      guard wrapper is Object(wrapped) else {
        raise @mcap.io_error("invalid decoded normalization input")
      }
      let normalized = normalizer.normalize(wrapped["data"])
      let payload = normalized.value.stringify()
      if !filter.matches(@utf8.encode(payload[:])) {
        continue
      }
      guard declarations.channels
        .iter()
        .find_first(fn(c) { c.id == message.channel_id })
        is Some(channel) else {
        raise @mcap.io_error("undeclared normalization channel")
      }
      let row : Json = {
        "topic": channel.topic,
        "sequence": message.sequence.to_string(),
        "log_time": message.log_time.to_string(),
        "publish_time": message.publish_time.to_string(),
        "data": normalized.value,
        "defaulted": normalized.defaulted,
        "missing": normalized.missing,
      }
      transaction.sink().write(@utf8.encode((row.stringify() + "\n")[:]))
      rows += 1UL
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
      { "source_sha256": sha256, "rows": rows.to_string(), "output": values[3] } :
      Json).stringify(),
  )
}
