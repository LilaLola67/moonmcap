///|
fn tf_file(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  let config = object(json_file(values[2]), [
    "queries", "frames", "start", "end", "tf_start", "tf_end", "topics", "sample_limit",
    "byte_limit", "copy_auxiliary",
  ])
  fn budget(key : String, fallback : Int) -> Int raise @mcap.McapError {
    match config.get(key) {
      None => fallback
      Some(Number(_, ..) as value) => {
        let n = @mcap.parse_decimal(value.stringify())
        if n > 2147483647UL {
          raise @mcap.io_error("TF budget exceeds Int")
        }
        n.to_int()
      }
      _ => raise @mcap.io_error("integer TF budget required")
    }
  }
  fn strings(key : String) -> Array[String] raise @mcap.McapError {
    match config.get(key) {
      None => []
      Some(Array(items)) =>
        items.map(fn(v) {
          match v {
            String(s) => s
            _ => raise @mcap.io_error("string array required")
          }
        })
      _ => raise @mcap.io_error("string array required")
    }
  }
  let input = scope.open(values[1])
  let limits = { ..@mcap.default_limits(), max_records: 30000000, }
  ignore(@mcap.scan_source(input, @mcap.source_visitor(), limits~))
  let source = @mcap.open_reader(input, limits~).query_source()
  let template = source.declaration_log()
  let parser = @mcap.json_filter("true")
  let decoder = @mcap.standard_payload_decoder()
  let tree = @mcap.tf_tree(
    sample_limit=budget("sample_limit", 1000000),
    byte_limit=budget("byte_limit", 128 * 1024 * 1024),
  )
  let stream = source.stream(query={
    ..@mcap.all_query(),
    topics: ["/tf", "/tf_static"],
    limit: 2147483647,
  })
  defer stream.cancel()
  while stream.next() is Some(message) {
    let decoded = decoder.decode_message(message, template)
    guard parser.project(decoded.data) is Object(fields) else {
      raise @mcap.io_error("invalid TF decoded JSON")
    }
    let is_static = template.channels.any(fn(c) {
      c.id == message.channel_id && c.topic == "/tf_static"
    })
    for
      sample in @mcap.parse_tf_message(
        fields["data"],
        is_static,
        origin=message,
      ) {
      tree.add(sample)
    }
  }
  let sha256 = @nodeio.source_sha256(input)
  if values[0] == "tf-query" {
    guard config.get("queries") is Some(Array(queries)) &&
      !queries.is_empty() &&
      queries.length() <= 10000 else {
      raise @mcap.io_error("1..10000 TF queries required")
    }
    let reports = queries.map(fn(value) {
      let q = object(value, [
        "target", "source", "time", "max_gap", "interpolate",
      ])
      let time = @mcap.parse_decimal(text(q, "time"))
      let max_gap = if q.contains("max_gap") {
        @mcap.parse_decimal(text(q, "max_gap"))
      } else {
        18446744073709551615UL
      }
      let result = tree.lookup(
        text(q, "target"),
        text(q, "source"),
        time,
        max_gap~,
        interpolate=boolean(q, "interpolate", true),
      )
      (
        {
          "target": text(q, "target"),
          "source": text(q, "source"),
          "time": time.to_string(),
          "transform": @mcap.tf_transform_json(result.transform),
          "interpolated": result.interpolated,
          "evidence": result.evidence.map(@mcap.tf_stamped_json),
        } : Json)
    })
    @nodeio.write_atomic_new(
      values[3],
      @utf8.encode(
        (({ "source_sha256": sha256, "queries": reports } : Json).stringify() +
        "\n")[:],
      ),
    )
  } else {
    let start = @mcap.parse_decimal(text(config, "start"))
    let end = @mcap.parse_decimal(text(config, "end"))
    if start > end {
      raise @mcap.io_error("invalid TF clip log-time window")
    }
    let tf_start = @mcap.parse_decimal(text(config, "tf_start"))
    let tf_end = @mcap.parse_decimal(text(config, "tf_end"))
    let context = tree.context(strings("frames"), tf_start, tf_end)
    let retained : Map[String, Array[@mcap.Message]] = {}
    fn key(message : @mcap.Message) -> String {
      message.channel_id.to_string() +
      ":" +
      message.sequence.to_string() +
      ":" +
      message.log_time.to_string()
    }
    for sample in context {
      if sample.origin is Some(message) {
        let id = key(message)
        match retained.get(id) {
          Some(items) => if !items.contains(message) { items.push(message) }
          None => retained[id] = [message]
        }
      }
    }
    let topics = strings("topics")
    for topic in topics {
      if !template.channels.any(fn(c) { c.topic == topic }) {
        raise @mcap.io_error("TF clip topic absent")
      }
    }
    let selected : Map[Int, Bool] = {}
    for channel in template.channels {
      if topics.is_empty() || topics.contains(channel.topic) {
        selected[channel.id] = true
      }
    }
    let transaction = @nodeio.output_transaction(values[3])
    try {
      let writer = @mcap.new_stream_writer(
        transaction.sink(),
        profile=template.header.profile,
      )
      for schema in template.schemas {
        writer.add_schema(schema)
      }
      for channel in template.channels {
        writer.add_channel(channel)
      }
      let all = source.stream(query={ ..@mcap.all_query(), limit: 2147483647, })
      defer all.cancel()
      while all.next() is Some(message) {
        let semantic = match retained.get(key(message)) {
          Some(items) => items.contains(message)
          None => false
        }
        if semantic ||
          (
            message.log_time >= start &&
            message.log_time <= end &&
            selected.contains(message.channel_id)
          ) {
          writer.append(message)
        }
      }
      if boolean(config, "copy_auxiliary", false) {
        ignore(@mcap.copy_clip_auxiliary(input, writer, limits~))
      }
      writer.add_metadata({
        name: "moonmcap.tf-context",
        entries: [
          ("source_sha256", sha256),
          (
            "manifest",
            (
              {
                "start": start.to_string(),
                "end": end.to_string(),
                "tf_start": tf_start.to_string(),
                "tf_end": tf_end.to_string(),
                "evidence": context.map(@mcap.tf_stamped_json),
                "copy_auxiliary": boolean(config, "copy_auxiliary", false),
              } : Json).stringify(),
          ),
        ],
      })
      ignore(writer.finish())
      transaction.publish()
    } catch {
      error => {
        transaction.abort() catch {
          _ => ()
        }
        raise error
      }
    }
  }
  println(({ "source_sha256": sha256, "output": values[3] } : Json).stringify())
}
