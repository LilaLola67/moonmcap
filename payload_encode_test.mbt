///|
fn encoded_has(bytes : Bytes, part : Bytes) -> Bool {
  for i = 0; i + part.length() <= bytes.length(); i = i + 1 {
    if bytes[i:i + part.length()].to_owned() == part {
      return true
    }
  }
  false
}

///|
test "CDR and ROS1 encoders roundtrip exact types endian strings and arrays" {
  let definition = b"uint8 prefix\nint64 low\nuint64 high\nfloat64 x\nstring label\nuint16[2] fixed\nint32[] data"
  let value = @json.parse(
    "{\"prefix\":255,\"low\":\"-9223372036854775808\",\"high\":\"18446744073709551615\",\"x\":1.25,\"label\":\"月球\",\"fixed\":[0,65535],\"data\":[-2147483648,2147483647]}",
  )
  let options = { ..@moonmcap.cdr_options(), int64_as_string: true, }
  let cdr = @moonmcap.cdr_decoder("demo/Value", definition, options~)
  for little_endian in [false, true] {
    let bytes = cdr.encode(value, little_endian~)
    assert_eq(bytes[1], if little_endian { 0x01 } else { 0x00 })
    assert_eq(cdr.decode(bytes), value)
  }
  let ros1 = @moonmcap.ros1_decoder("demo/Value", definition, options~)
  assert_eq(ros1.decode(ros1.encode(value)), value)
  assert_eq(
    @moonmcap.cdr_decoder("demo/Empty", b"").encode(@json.parse("{}")),
    b"\x00\x01\x00\x00\x00",
  )
}

///|
test "typed encoders reject range shape schema and resource violations" {
  let decoder = @moonmcap.cdr_decoder(
    "demo/Value", b"uint8 n\nuint16[2] fixed\nstring<=3 text",
  )
  for
    bad in [
      "{\"n\":256,\"fixed\":[0,0],\"text\":\"a\"}", "{\"n\":-1,\"fixed\":[0,0],\"text\":\"a\"}",
      "{\"n\":1.5,\"fixed\":[0,0],\"text\":\"a\"}", "{\"n\":1,\"fixed\":[0],\"text\":\"a\"}",
      "{\"n\":1,\"fixed\":[0,0],\"text\":\"long\"}", "{\"n\":1,\"fixed\":[0,0],\"text\":\"a\\u0000b\"}",
      "{\"n\":1,\"fixed\":[0,0],\"text\":\"a\",\"extra\":1}", "{\"n\":1,\"fixed\":[0,0]}",
    ] {
    try decoder.encode(@json.parse(bad)) catch {
      _ => ()
    } noraise {
      _ => fail("invalid typed value accepted")
    }
  }
  let small = @moonmcap.cdr_decoder("demo/Text", b"string text", options={
    ..@moonmcap.cdr_options(),
    message_bytes: 8,
  })
  try small.encode(@json.parse("{\"text\":\"abc\"}")) catch {
    _ => ()
  } noraise {
    _ => fail("byte budget ignored")
  }
  let time = @moonmcap.ros1_decoder("demo/Time", b"time stamp")
  try
    time.encode(@json.parse("{\"stamp\":{\"sec\":1,\"nanosec\":1000000000}}"))
  catch {
    _ => ()
  } noraise {
    _ => fail("invalid ROS1 timestamp accepted")
  }
}

///|
test "protobuf encoder handles scalar extrema maps repeated nested and oneof" {
  let decoder = @moonmcap.protobuf_decoder(protobuf_fixture(), options={
    ..@moonmcap.protobuf_options(),
    int64_as_string: true,
  })
  let value = @json.parse(
    "{\"f1\":1.25,\"f2\":-2.5,\"f3\":\"-9223372036854775808\",\"f4\":\"18446744073709551615\",\"f5\":-2147483648,\"f6\":\"9007199254740993\",\"f7\":4294967295,\"f8\":true,\"f9\":\"月球\",\"f10\":\"AP8B\",\"f11\":4294967295,\"f12\":\"RUN\",\"f13\":-2147483648,\"f14\":\"-9223372036854775808\",\"f15\":-2147483648,\"f16\":\"-9223372036854775808\",\"child\":{\"a\":7,\"b\":\"nested\"},\"values\":[\"-1\",\"0\",\"9223372036854775807\"],\"labels\":{\"x\":\"9007199254740993\",\"y\":\"-9\"},\"code\":8}",
  )
  assert_eq(
    decoder.decode(
      "telemetry.Sample",
      decoder.encode("telemetry.Sample", value),
    ),
    value,
  )
  for
    bad in [
      "{\"f5\":2147483648}", "{\"f4\":\"18446744073709551616\"}", "{\"f8\":1}", "{\"f10\":\"?\"}",
      "{\"text\":\"x\",\"code\":1}", "{\"unknown\":1}",
    ] {
    try decoder.encode("telemetry.Sample", @json.parse(bad)) catch {
      _ => ()
    } noraise {
      _ => fail("invalid protobuf input accepted")
    }
  }
}

///|
test "protobuf edits preserve unknown fields including nested fields and groups" {
  let decoder = @moonmcap.protobuf_decoder(protobuf_fixture())
  let unknown = b"\xa0\x06\x2a\xab\x06\x08\x01\xac\x06"
  let original = unknown +
    b"\x8a\x01\x08\x08\x07\x98\x06\x2a\x12\x01x\xa8\x01\x08"
  let changed = decoder.rewrite("telemetry.Sample", original, [
    { path: "/child/a", replacement: Some(Json::number(9)), },
  ])
  assert_true(encoded_has(changed, unknown))
  assert_true(encoded_has(changed, b"\x98\x06\x2a"))
  assert_eq(
    decoder.decode("telemetry.Sample", changed),
    @json.parse("{\"child\":{\"a\":9,\"b\":\"x\"},\"code\":8}"),
  )
  let selected = decoder.rewrite("telemetry.Sample", changed, [
    { path: "/text", replacement: Some(Json::string("redacted")), },
  ])
  assert_eq(
    decoder.decode("telemetry.Sample", selected),
    @json.parse("{\"child\":{\"a\":9,\"b\":\"x\"},\"text\":\"redacted\"}"),
  )
  let removed = decoder.rewrite("telemetry.Sample", selected, [
    { path: "/text", replacement: None, },
  ])
  assert_eq(
    decoder.decode("telemetry.Sample", removed),
    @json.parse("{\"child\":{\"a\":9,\"b\":\"x\"}}"),
  )
  assert_eq(decoder.rewrite("telemetry.Sample", original, []), original)
}

///|
test "JSON payload edits preserve input values and reject ambiguous paths" {
  let original = @json.parse("{\"a\":{\"b\":1},\"items\":[1,2,3]}")
  let changed = @moonmcap.edit_payload_json(original, [
    { path: "/a/b", replacement: Some(Json::number(2)), },
    { path: "/items/1", replacement: None, },
  ])
  assert_eq(changed, @json.parse("{\"a\":{\"b\":2},\"items\":[1,3]}"))
  assert_eq(original, @json.parse("{\"a\":{\"b\":1},\"items\":[1,2,3]}"))
  let invalid : Array[Array[@moonmcap.PayloadEdit]] = [
    [{ path: "/a", replacement: None, }, { path: "/a/b", replacement: None, }],
    [{ path: "/items/01", replacement: None, }],
    [{ path: "/missing/child", replacement: Some(Json::number(1)), }],
  ]
  for edits in invalid {
    try @moonmcap.edit_payload_json(original, edits) catch {
      _ => ()
    } noraise {
      _ => fail("invalid edit accepted")
    }
  }
}
