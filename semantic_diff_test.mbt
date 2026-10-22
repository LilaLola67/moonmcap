///|
test "semantic diff ignores compression IDs and tied message physical order" {
  let original = example_log()
  let changed = {
    ..original,
    schemas: original.schemas.map(fn(s) { { ..s, id: s.id + 20, } }),
    channels: original.channels.map(fn(c) {
      {
        ..c,
        id: c.id + 10,
        schema_id: if c.schema_id == 0 {
          0
        } else {
          c.schema_id + 20
        },
      }
    }),
    messages: original.messages.map(fn(m) {
      { ..m, channel_id: m.channel_id + 10, }
    }),
  }
  let a = @moonmcap.memory_source(
    @moonmcap.write_indexed(original, compression=@moonmcap.Lz4),
  )
  let b = @moonmcap.memory_source(
    @moonmcap.write_indexed(
      changed,
      compression=@moonmcap.Zstd,
      messages_per_chunk=1,
    ),
  )
  let result = @moonmcap.diff_sources(a, b)
  guard result is Object(fields) else { fail("diff result") }
  assert_eq(fields["equal"], Json::boolean(true))
  assert_eq(
    fields["matching_messages"],
    Json::number(original.messages.length().to_double()),
  )
}

///|
test "semantic diff preserves duplicates and optionally normalizes JSON" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/json", "json")
  builder.append(channel, 1UL, b"{\"a\":1,\"b\":2}")
  builder.append(channel, 1UL, b"{\"a\":1,\"b\":2}")
  let log = builder.snapshot()
  let changed = {
    ..log,
    messages: log.messages.map(fn(m) {
      { ..m, data: b"{\"b\":2.0,\"a\":1e0}", }
    }),
  }
  let a = @moonmcap.memory_source(@moonmcap.write_indexed(log))
  let b = @moonmcap.memory_source(@moonmcap.write_indexed(changed))
  guard @moonmcap.diff_sources(a, b) is Object(raw) else { fail("raw diff") }
  assert_eq(raw["equal"], Json::boolean(false))
  guard @moonmcap.diff_sources(a, b, options={
      ..@moonmcap.diff_options(),
      json_payloads: true,
    })
    is Object(normal) else {
    fail("json diff")
  }
  assert_eq(normal["equal"], Json::boolean(true))
  let fewer = { ..changed, messages: changed.messages[0:1].to_array(), }
  guard @moonmcap.diff_sources(
      a,
      @moonmcap.memory_source(@moonmcap.write_indexed(fewer)),
      options={
        ..@moonmcap.diff_options(),
        json_payloads: true,
        max_details: 0,
      },
    )
    is Object(diff) else {
    fail("duplicate diff")
  }
  assert_eq(diff["left_only_messages"], Json::number(1))
  assert_eq(diff["omitted_details"], Json::number(1))
  assert_eq(diff["details"], Json::array([]))
  try
    @moonmcap.diff_sources(a, b, options={
      ..@moonmcap.diff_options(),
      auxiliary_bytes: 1,
    })
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("diff budget")
  }
}
