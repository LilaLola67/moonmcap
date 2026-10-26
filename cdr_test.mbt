///|
test "CDR budgets and unsupported representations fail explicitly" {
  let schema = b"string text\nuint64 counter"
  for
    options in [
      { ..@moonmcap.cdr_options(), schema_bytes: 1, },
      { ..@moonmcap.cdr_options(), fields: 1, },
      { ..@moonmcap.cdr_options(), depth: 0, },
    ] {
    try @moonmcap.cdr_decoder("demo/msg/Value", schema, options~) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("CDR schema budget ignored")
    }
  }
  let decoder = @moonmcap.cdr_decoder("demo/msg/Text", b"string text", options={
    ..@moonmcap.cdr_options(),
    string_bytes: 2,
  })
  for
    bad in [
      b"\x00\x01\x00\x00\xff\xff\xff\xff", b"\x00\x01\x00\x00\x04\x00\x00\x00abc\x00",
      b"\x00\x01\x00\x00\x02\x00\x00\x00\xff\x00", b"\x00\x07\x00\x00", b"\x00\x01\x00\x01",
    ] {
    try decoder.decode(bad) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid CDR accepted")
    }
  }
  let limited = @moonmcap.cdr_decoder("demo/msg/Array", b"uint8[] values", options={
    ..@moonmcap.cdr_options(),
    items: 2,
  })
  try limited.decode(b"\x00\x01\x00\x00\x03\x00\x00\x00\x01\x02\x03") catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("sequence budget ignored")
  }
  // ROS 2 rosidl maps byte to IDL octet (unsigned), independent of ROS 1 byte aliases.
  assert_eq(
    @moonmcap.cdr_decoder("demo/msg/Byte", b"byte value").decode(
      b"\x00\x01\x00\x00\xff",
    ),
    @json.parse("{\"value\":255}"),
  )
}

///|
test "CDR payload adapter preserves exact uint64 aggregation" {
  let builder = @moonmcap.new_builder()
  let sid = builder.register_schema("demo/msg/Counter", "ros2msg", b"uint64 n")
  let cid = builder.register_channel("/cdr", "cdr", schema_id=sid)
  builder.append(cid, 1UL, b"\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x20\x00")
  builder.append(cid, 2UL, b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01")
  let dataset = @moonmcap.open_dataset([
    @moonmcap.build_query_index(builder.snapshot())
    .query_source()
    .decode_payload(@moonmcap.standard_payload_decoder()),
  ])
  let report = @moonmcap.aggregate_json(
    dataset.cursor(@moonmcap.all_query()),
    dataset.channels(),
    options={ ..@moonmcap.aggregate_options(), value_path: "/n", },
  )
  assert_true(report is { "groups": [{ "sum": "9007199254740994e0", .. }], .. })
}
