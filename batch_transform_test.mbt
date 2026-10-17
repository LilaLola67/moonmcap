///|
test "batch sampling precedes pagination and topic mappings are simultaneous" {
  let log = cursor_fixture()
  let options = {
    ..@moonmcap.transform_options(),
    query: {
      ..@moonmcap.all_query(),
      topics: ["/shared"],
      descending: true,
      offset: 1,
      limit: 5,
    },
    topic_map: [("/shared", "/renamed")],
    sample_period: 10UL,
  }
  let result = @moonmcap.transform_log(log, options)
  assert_eq(result.messages.length(), 5)
  assert_true(result.channels.all(fn(c) { c.topic == "/renamed" }))
  let all = @moonmcap.transform_log(log, {
    ..options,
    query: { ..options.query, descending: false, offset: 0, limit: 1000, },
  })
  assert_eq(all.messages.length(), 9)
  let expected = all.messages
  for i = 0; i < 5; i = i + 1 {
    assert_eq(result.messages[i], expected[expected.length() - 2 - i])
  }
  assert_true(log.channels.any(fn(c) { c.topic == "/shared" }))
}

///|
test "renaming colliding topics preserves channels and schema references" {
  let log = example_log()
  let original = log.channels.map(fn(c) { c.topic })
  let result = @moonmcap.transform_log(log, {
    ..@moonmcap.transform_options(),
    topic_map: [(original[0], original[1]), (original[1], original[0])],
  })
  assert_eq(result.channels[0].topic, original[1])
  assert_eq(result.channels[1].topic, original[0])
  assert_eq(
    result.channels.map(fn(c) { c.id }),
    log.channels.map(fn(c) { c.id }),
  )
  assert_eq(result.schemas, log.schemas)
}

///|
test "batch filters attachments metadata and shifts all retained clocks" {
  let log = example_log()
  log.attachments.push({
    log_time: 15UL,
    create_time: 12UL,
    name: "keep",
    media_type: "json",
    data: b"{}",
  })
  log.attachments.push({
    log_time: 25UL,
    create_time: 22UL,
    name: "drop",
    media_type: "raw",
    data: b"x",
  })
  log.metadata.push({
    name: "keep",
    entries: [("run", "1"), ("extra", "retained")],
  })
  log.metadata.push({ name: "keep", entries: [("run", "2")], })
  let result = @moonmcap.transform_log(log, {
    ..@moonmcap.transform_options(),
    align_start: Some(100UL),
    attachment_names: ["keep"],
    attachment_media_types: ["json"],
    attachment_start: 15UL,
    attachment_end: 15UL,
    metadata_names: ["keep"],
    metadata_entries: [("run", "1")],
  })
  assert_eq(result.messages[0].log_time, 100UL)
  assert_eq(result.attachments.length(), 1)
  assert_eq(result.attachments[0].log_time, 105UL)
  assert_eq(result.attachments[0].create_time, 102UL)
  assert_eq(result.metadata.length(), 1)
  assert_eq(result.metadata[0].entries.length(), 2)
  assert_eq(log.attachments[0].log_time, 15UL)
}

///|
test "batch rejects invalid maps and clock overflow without mutating input" {
  let log = example_log()
  let before = @moonmcap.copy_log(log)
  for
    options in [
      { ..@moonmcap.transform_options(), topic_map: [("/missing", "/new")], },
      { ..@moonmcap.transform_options(), topic_map: [("/events", "")], },
      {
        ..@moonmcap.transform_options(),
        topic_map: [("/events", "/x"), ("/events", "/y")],
      },
      {
        ..@moonmcap.transform_options(),
        attachment_start: 2UL,
        attachment_end: 1UL,
      },
      {
        ..@moonmcap.transform_options(),
        align_start: Some(18446744073709551615UL),
      },
      {
        ..@moonmcap.transform_options(),
        metadata_entries: [("a", "1"), ("a", "2")],
      },
    ] {
    try @moonmcap.transform_log(log, options) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid transform accepted")
    }
    assert_eq(log, before)
  }
}

///|
test "sampling handles UInt64 origin extremes and prunes unused declarations" {
  let log = cursor_fixture()
  let none = @moonmcap.transform_log(log, {
    ..@moonmcap.transform_options(),
    sample_period: 2UL,
    sample_origin: 18446744073709551615UL,
  })
  assert_eq(none.messages, [])
  assert_eq(none.channels, [])
  assert_eq(none.schemas, [])
  let result = @moonmcap.transform_log(log, {
    ..@moonmcap.transform_options(),
    sample_period: 18446744073709551615UL,
  })
  assert_eq(result.messages.length(), 8)
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    assert_eq(
      @moonmcap.read_log(@moonmcap.write_indexed(result, compression~)).messages,
      result.messages,
    )
  }
}
