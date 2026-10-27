///|
test "ROS1 numeric decoding composes with exact aggregation" {
  let builder = @moonmcap.new_builder()
  let schema = builder.register_schema("demo/Counter", "ros1msg", b"uint64 n")
  let channel = builder.register_channel("/counter", "ros1", schema_id=schema)
  builder.append(channel, 1UL, b"\x01\x00\x00\x00\x00\x00\x20\x00")
  builder.append(channel, 2UL, b"\x01\x00\x00\x00\x00\x00\x00\x00")
  let dataset = @moonmcap.open_dataset([
    @moonmcap.build_query_index(builder.snapshot())
    .query_source()
    .decode_payload(@moonmcap.standard_payload_decoder()),
  ])
  assert_true(
    @moonmcap.aggregate_json(
      dataset.cursor(@moonmcap.all_query()),
      dataset.channels(),
      options={ ..@moonmcap.aggregate_options(), value_path: "/n", },
    )
    is { "groups": [{ "sum": "9007199254740994e0", .. }], .. },
  )
}

///|
test "ROS1 strings aliases times unaligned values and empty messages" {
  assert_eq(
    @moonmcap.ros1_decoder("demo/Empty", b"").decode(b""),
    @json.parse("{}"),
  )
  assert_eq(
    @moonmcap.ros1_decoder("demo/Zero", b"uint8[0] empty\nuint32 n").decode(
      b"\x07\x00\x00\x00",
    ),
    @json.parse("{\"empty\":[],\"n\":7}"),
  )
  let decoder = @moonmcap.ros1_decoder(
    "demo/Value", b"byte b\nchar c\nuint32 n\nstring text\ntime stamp\nduration delta",
  )
  let bytes = b"\xff\xff\x78\x56\x34\x12\x00\x00\x00\x00\x02\x00\x00\x00\x03\x00\x00\x00\xff\xff\xff\xff\x01\x00\x00\x00"
  let expected = @json.parse(
    "{\"b\":-1,\"c\":255,\"n\":305419896,\"text\":\"\",\"stamp\":{\"sec\":2,\"nanosec\":3},\"delta\":{\"sec\":-1,\"nanosec\":1}}",
  )
  assert_eq(decoder.decode(bytes), expected)
  assert_eq(decoder.view(bytes).json(), expected)
  for bad in [bytes + b"\x00", bytes[0:bytes.length() - 1].to_owned()] {
    try decoder.decode(bad) catch {
      _ => ()
    } noraise {
      _ => fail("invalid ROS1 tail accepted")
    }
  }
  let time = @moonmcap.ros1_decoder("demo/Time", b"time value")
  assert_eq(
    time.decode(b"\xff\xff\xff\xff\x00\x00\x00\x00"),
    @json.parse("{\"value\":{\"sec\":4294967295,\"nanosec\":0}}"),
  )
  let bad = b"\x00\x00\x00\x00\x00\xca\x9a\x3b"
  try time.decode(bad) catch {
    _ => ()
  } noraise {
    _ => fail("invalid ROS1 nanoseconds")
  }
  try
    time.view(bad, selection={
      ..@moonmcap.decode_selection(),
      paths: ["/absent"],
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("unselected invalid ROS1 nanoseconds")
  }
  let view = @moonmcap.ros1_decoder("demo/Array", b"uint8 tag\nuint32[] values").view(
    b"\x01\x02\x00\x00\x00\x78\x56\x34\x12\xff\xff\xff\xff",
    selection={ ..@moonmcap.decode_selection(), inline_array_items: 0, },
  )
  assert_eq(
    view.read_elements("/values", 0, 2),
    @json.parse("[305419896,4294967295]"),
  )
}
