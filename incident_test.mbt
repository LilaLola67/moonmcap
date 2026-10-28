///|
fn incident_fixture() -> @moonmcap.Log raise {
  let builder = @moonmcap.new_builder()
  let a = builder.register_channel("/sensor", "json")
  let b = builder.register_channel("/state", "json")
  for t in [0UL, 10UL, 20UL, 30UL, 40UL, 50UL, 60UL] {
    builder.append(
      a,
      t,
      if t >= 20UL && t <= 40UL {
        b"{\"v\":25}"
      } else {
        b"{\"v\":0}"
      },
    )
    if t == 0UL || t == 10UL {
      builder.append(b, t, b"{\"mode\":1}")
    }
  }
  builder.snapshot()
}

///|
fn incident_json(value : Json, key : String) -> Json {
  match value {
    Object(fields) => fields[key]
    _ => Json::null()
  }
}

///|
test "quality distinguishes silent edges failed fields missing topics and warning rules" {
  let source = @moonmcap.build_query_index(incident_fixture()).query_source()
  let rule = {
    ..@moonmcap.quality_rule("/sensor"),
    max_gap: Some(10UL),
    check_sequence: true,
    condition: Some("{\"path\":\"/v\",\"op\":\">=\",\"value\":0}"),
  }
  assert_eq(
    incident_json(@moonmcap.check_quality(source, [rule], 0UL, 60UL), "status"),
    Json::string("passed"),
  )
  let warning = { ..@moonmcap.quality_rule("/missing"), warning: true, }
  let report = @moonmcap.check_quality(
    source,
    [rule, warning],
    0UL,
    80UL,
    detail_limit=0,
  )
  assert_eq(incident_json(report, "status"), Json::string("failed"))
  assert_eq(
    incident_json(
      @moonmcap.check_quality(source, [warning], 0UL, 60UL),
      "status",
    ),
    Json::string("warning"),
  )
  let condition = {
    ..rule,
    condition: Some("{\"path\":\"/required\",\"op\":\"exists\"}"),
  }
  assert_eq(
    incident_json(
      @moonmcap.check_quality(source, [condition], 0UL, 60UL),
      "status",
    ),
    Json::string("failed"),
  )
}

///|
test "events require sampled continuity and duration retain incomplete spans and merge windows" {
  let source = @moonmcap.build_query_index(incident_fixture()).query_source()
  let rule = {
    ..@moonmcap.event_rule(
      "/sensor", "{\"path\":\"/v\",\"op\":\">\",\"value\":20}",
    ),
    duration: 10UL,
    max_gap: 10UL,
  }
  let events = @moonmcap.detect_events(source, rule)
  assert_eq(events.length(), 1)
  assert_eq(events[0].start, 20UL)
  assert_eq(events[0].confirmed, 30UL)
  assert_eq(events[0].end, 50UL)
  assert_true(events[0].closed)
  let partial = @moonmcap.detect_events(source, rule, end=40UL)
  assert_eq(partial.length(), 1)
  assert_true(!partial[0].closed)
  let sparse = @moonmcap.detect_events(source, { ..rule, max_gap: 9UL, })
  assert_eq(sparse.length(), 0)
  let windows = @moonmcap.event_windows(
    [events[0], { start: 60UL, confirmed: 60UL, end: 65UL, closed: true, }],
    5UL,
    5UL,
  )
  assert_eq(windows.length(), 1)
  assert_eq(windows[0].events.length(), 2)
  assert_eq(windows[0].start, 15UL)
  assert_eq(windows[0].end, 70UL)
}

///|
test "event hysteresis gap cooldown and finish preserve explicit evidence" {
  let detector = @moonmcap.event_detector({
    ..@moonmcap.event_rule("/x", "{\"path\":\"/v\",\"op\":\">\",\"value\":20}"),
    leave: Some("{\"path\":\"/v\",\"op\":\"<\",\"value\":10}"),
    max_gap: 10UL,
    cooldown: 20UL,
  })
  let m : @moonmcap.Message = {
    channel_id: 1,
    sequence: 0U,
    log_time: 0UL,
    publish_time: 0UL,
    data: b"{\"v\":25}",
  }
  assert_eq(detector.feed(m).length(), 1)
  assert_eq(
    detector.feed({ ..m, log_time: 5UL, data: b"{\"v\":15}", }).length(),
    0,
  )
  let closed = detector.feed({ ..m, log_time: 20UL, })
  assert_true(
    closed[0] is @moonmcap.EventClosed(span) && !span.closed && span.end == 5UL,
  )
  assert_eq(detector.feed({ ..m, log_time: 24UL, }).length(), 0)
  assert_eq(detector.feed({ ..m, log_time: 25UL, }).length(), 1)
  assert_true(
    detector.finish() is Some(span) && span.start == 25UL && !span.closed,
  )
  assert_eq(detector.finish(), None)
}

///|
test "context clips preserve raw bytes prior state missing policy and output budget" {
  let log = incident_fixture()
  let source = @moonmcap.build_query_index(log).query_source()
  let clip = @moonmcap.extract_context_clip(
    source,
    20UL,
    40UL,
    topics=["/sensor"],
    context=[{ ..@moonmcap.context_rule("/state"), depth: 2, }],
  )
  assert_eq(clip.log.messages.length(), 5)
  assert_eq(clip.log.messages.map(fn(m) { m.log_time }), [
    0UL, 10UL, 20UL, 30UL, 40UL,
  ])
  assert_eq(
    @moonmcap.read_log(
      @moonmcap.write_indexed(clip.log, compression=@moonmcap.Zstd),
    ).messages,
    clip.log.messages,
  )
  try
    @moonmcap.extract_context_clip(source, 20UL, 40UL, context=[
      { ..@moonmcap.context_rule("/state"), max_age: 1UL, },
    ])
  catch {
    _ => ()
  } noraise {
    _ => fail("missing required context accepted")
  }
  try @moonmcap.extract_context_clip(source, 20UL, 40UL, byte_limit=32) catch {
    _ => ()
  } noraise {
    _ => fail("clip byte budget ignored")
  }
}

///|
test "trigger buffers account eviction merge triggers and signal short post history" {
  let log = incident_fixture()
  let clips : Array[@moonmcap.Log] = []
  let reports : Array[Json] = []
  let recorder = @moonmcap.trigger_recorder(
    { ..log, messages: [], },
    fn(clip, report) {
      clips.push(clip)
      reports.push(report)
    },
    options={
      ..@moonmcap.trigger_options(),
      pre: 20UL,
      post: 10UL,
      buffer_messages: 2,
    },
  )
  for
    message in log.messages.filter(fn(m) { m.channel_id == log.channels[0].id }) {
    recorder.feed(message)
    if message.log_time == 30UL || message.log_time == 40UL {
      recorder.trigger("test")
    }
  }
  recorder.finish()
  assert_eq(clips.length(), 1)
  assert_eq(clips[0].messages.map(fn(m) { m.log_time }), [
    20UL, 30UL, 40UL, 50UL,
  ])
  assert_eq(incident_json(reports[0], "pre_complete"), Json::boolean(false))
  assert_eq(incident_json(reports[0], "post_complete"), Json::boolean(true))
  let short = @moonmcap.trigger_recorder({ ..log, messages: [], }, fn(
    _,
    report,
  ) {
    reports.push(report)
  })
  short.feed(log.messages[0])
  short.trigger("short")
  short.finish()
  assert_eq(incident_json(reports[1], "post_complete"), Json::boolean(false))
}

///|
test "trigger emitter failures latch failed state release buffers and never retry publication" {
  let log = incident_fixture()
  let calls = [0]
  let recorder = @moonmcap.trigger_recorder(
    { ..log, messages: [], },
    fn(_, _) {
      calls[0] += 1
      raise @moonmcap.io_error("sink failed")
    },
    options={ ..@moonmcap.trigger_options(), post: 0UL, },
  )
  recorder.feed(log.messages[0])
  recorder.trigger("fault")
  try recorder.finish() catch {
    _ => ()
  } noraise {
    _ => fail("sink error swallowed")
  }
  assert_eq(calls[0], 1)
  assert_eq(incident_json(recorder.progress(), "state"), Json::string("failed"))
  assert_eq(
    incident_json(recorder.progress(), "buffered_bytes"),
    Json::number(0),
  )
}

///|
test "piecewise clocks round exactly preserve payload timestamps and apply pagination after mapping" {
  let map = @moonmcap.clock_map([
    { source: 0UL, target: 100UL, },
    { source: 30UL, target: 120UL, },
    { source: 60UL, target: 180UL, },
  ])
  assert_eq(map.map(29UL), 119UL)
  assert_eq(map.map(45UL), 150UL)
  let log = incident_fixture()
  let corrected = @moonmcap.calibrate_log(log, map)
  assert_eq(corrected.messages[0].publish_time, log.messages[0].publish_time)
  assert_eq(corrected.messages[0].data, log.messages[0].data)
  let provider = @moonmcap.build_query_index(log)
    .query_source()
    .calibrate_time(map)
  let stream = provider.stream(query={
    ..@moonmcap.all_query(),
    topics: ["/sensor"],
    start_time: 120UL,
    end_time: 180UL,
    offset: 1,
    limit: 1,
  })
  assert_true(
    stream.next() is Some(m) && m.log_time == 140UL && m.publish_time == 40UL,
  )
  assert_eq(stream.next(), None)
  let maximum = @moonmcap.clock_map([
    { source: 0UL, target: 0UL, },
    { source: 18446744073709551615UL, target: 18446744073709551615UL, },
  ])
  assert_eq(maximum.map(18446744073709551614UL), 18446744073709551614UL)
}

///|
test "clock extrapolation floors negatives rejects domains underflow and backwards anchors" {
  let map = @moonmcap.clock_map(
    [{ source: 10UL, target: 10UL, }, { source: 13UL, target: 12UL, }],
    extrapolate=true,
  )
  assert_eq(map.map(9UL), 9UL)
  try map.map(0UL) catch {
    _ => fail("valid extrapolation rejected")
  } noraise {
    t => assert_eq(t, 3UL)
  }
  let strict = @moonmcap.clock_map([
    { source: 10UL, target: 0UL, },
    { source: 20UL, target: 20UL, },
  ])
  try strict.map(9UL) catch {
    _ => ()
  } noraise {
    _ => fail("out of domain accepted")
  }
  try
    @moonmcap.clock_map([
      { source: 0UL, target: 2UL, },
      { source: 1UL, target: 1UL, },
    ])
  catch {
    _ => ()
  } noraise {
    _ => fail("backwards clock accepted")
  }
}

///|
test "quality matching counts every left message and closes empty sources" {
  let source = @moonmcap.build_query_index(incident_fixture()).query_source()
  let left = source.stream(query={
    ..@moonmcap.all_query(),
    topics: ["/sensor"],
  })
  let right = source.stream(query={
    ..@moonmcap.all_query(),
    topics: ["/state"],
  })
  let report = @moonmcap.check_match_quality(left, right, 0UL, 3000)
  assert_eq(incident_json(report, "basis_points"), Json::number(2857))
  assert_eq(incident_json(report, "status"), Json::string("failed"))
  let empty = source.stream(query={
    ..@moonmcap.all_query(),
    topics: ["/absent"],
  })
  let other = source.stream()
  assert_eq(
    incident_json(@moonmcap.check_match_quality(empty, other, 0UL, 0), "status"),
    Json::string("failed"),
  )
}

///|
test "cancelled checks close owned streams and event count budgets fail explicitly" {
  let log = incident_fixture()
  let stopped = [0]
  let provider = @moonmcap.query_source({ ..log, messages: [], }, fn(_, _) {
    let position = [0]
    @moonmcap.message_stream(
      fn() {
        if position[0] >= log.messages.length() {
          return None
        }
        let message = log.messages[position[0]]
        position[0] += 1
        Some(message)
      },
      fn() { stopped[0] += 1 },
    )
  })
  try
    @moonmcap.check_quality(
      provider,
      [@moonmcap.quality_rule("/sensor")],
      0UL,
      60UL,
      cancelled=fn() { true },
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("quality cancellation ignored")
  }
  assert_eq(stopped[0], 1)
  try
    @moonmcap.detect_events(provider, @moonmcap.event_rule("/sensor", "true"), cancelled=fn() {
      true
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("event cancellation ignored")
  }
  assert_eq(stopped[0], 2)
  let indexed = @moonmcap.build_query_index(log).query_source()
  try
    @moonmcap.detect_events(
      indexed,
      @moonmcap.event_rule(
        "/sensor", "{\"path\":\"/v\",\"op\":\"=\",\"value\":0}",
      ),
      event_limit=1,
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("event count limit ignored")
  }
  let emitted = [0]
  let recorder = @moonmcap.trigger_recorder({ ..log, messages: [], }, fn(_, _) {
    emitted[0] += 1
  })
  recorder.feed(log.messages[0])
  recorder.trigger("cancel")
  recorder.cancel()
  assert_eq(
    incident_json(recorder.progress(), "state"),
    Json::string("cancelled"),
  )
  assert_eq(emitted[0], 0)
}

///|
test "clock rounding ties use source chronological order in both views and rewritten files" {
  let log = incident_fixture()
  let unordered = {
    ..log,
    messages: [log.messages[4], log.messages[0], log.messages[2]],
  }
  let mapping = @moonmcap.clock_map([
    { source: 0UL, target: 100UL, },
    { source: 60UL, target: 101UL, },
  ])
  let rewritten = @moonmcap.calibrate_log(unordered, mapping)
  let stream = @moonmcap.build_query_index(unordered)
    .query_source()
    .calibrate_time(mapping)
    .stream()
  let viewed : Array[@moonmcap.Message] = []
  while stream.next() is Some(message) {
    viewed.push(message)
  }
  assert_eq(viewed, rewritten.messages)
  let descending = @moonmcap.build_query_index(unordered)
    .query_source()
    .calibrate_time(mapping)
    .stream(query={ ..@moonmcap.all_query(), descending: true, })
  assert_true(
    descending.next() is Some(m) && m.publish_time == viewed[2].publish_time,
  )
  descending.cancel()
  let overflow = @moonmcap.clock_map(
    [{ source: 10UL, target: 0UL, }, { source: 20UL, target: 20UL, }],
    extrapolate=true,
  )
  try overflow.map(9UL) catch {
    _ => ()
  } noraise {
    _ => fail("negative extrapolation accepted")
  }
}

///|
test "trigger oversize active windows fail rather than silently truncating" {
  let log = incident_fixture()
  let calls = [0]
  let recorder = @moonmcap.trigger_recorder(
    { ..log, messages: [], },
    fn(_, _) { calls[0] += 1 },
    options={ ..@moonmcap.trigger_options(), clip_messages: 1, },
  )
  recorder.feed(log.messages[0])
  recorder.trigger("budget")
  try recorder.feed(log.messages[1]) catch {
    _ => ()
  } noraise {
    _ => fail("active capture budget ignored")
  }
  assert_eq(calls[0], 0)
  assert_eq(incident_json(recorder.progress(), "state"), Json::string("failed"))
}
