///|
test "protobuf decode budgets reject malformed messages and unbounded descriptor states" {
  let descriptor = protobuf_fixture()
  for
    options in [
      { ..@moonmcap.protobuf_options(), descriptor_bytes: 1, },
      { ..@moonmcap.protobuf_options(), depth: 1, },
      { ..@moonmcap.protobuf_options(), fields: 1, },
      { ..@moonmcap.protobuf_options(), items: 1, },
    ] {
    try @moonmcap.protobuf_decoder(descriptor, options~) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("descriptor budget ignored")
    }
  }
  for
    (options, data) in [
      ({ ..@moonmcap.protobuf_options(), message_bytes: 1, }, b"\x20\x01"),
    ] {
    let limited = @moonmcap.protobuf_decoder(descriptor, options~)
    try limited.decode("telemetry.Sample", data) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("message budget ignored")
    }
  }
  let many = @buffer.Buffer::Buffer()
  for i = 0; i < 1001; i = i + 1 {
    many.write_bytes(b"\x20\x01")
  }
  let limited = @moonmcap.protobuf_decoder(descriptor, options={
    ..@moonmcap.protobuf_options(),
    items: 1000,
  })
  try limited.decode("telemetry.Sample", many.to_bytes()) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("item budget ignored")
  }
  let decoder = @moonmcap.protobuf_decoder(descriptor)
  for
    bad in [
      b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x02", b"\x00", b"\x4a\x01\xff", b"\x4c",
      b"\x8a\x01\x02\x80",
    ] {
    try decoder.decode("telemetry.Sample", bad) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("malformed wire accepted")
    }
  }
}

///|
test "decoded providers retain exact integers through filtering aggregation and source ID collisions" {
  let builder = @moonmcap.new_builder()
  let sid = builder.register_schema(
    "telemetry.Sample",
    "protobuf",
    protobuf_fixture(),
  )
  let id = builder.register_channel("/proto", "protobuf", schema_id=sid)
  builder.append(id, 1UL, b"\x20\x81\x80\x80\x80\x80\x80\x80\x10")
  builder.append(id, 2UL, b"\x20\x01")
  let decoder = @moonmcap.standard_payload_decoder(max_schemas=1)
  let provider = @moonmcap.build_query_index(builder.snapshot())
    .query_source()
    .decode_payload(decoder)
  let dataset = @moonmcap.open_dataset([provider])
  let result = @moonmcap.aggregate_json(
    dataset.cursor(@moonmcap.all_query()),
    dataset.channels(),
    options={ ..@moonmcap.aggregate_options(), value_path: "/f4", },
  )
  guard result is Object(fields) &&
    fields["groups"] is Array(groups) &&
    groups[0] is Object(row) else {
    fail("aggregate result")
  }
  assert_eq(row["sum"], Json::string("9007199254740994e0"))
  let filter = @moonmcap.json_filter(
    "{\"path\":\"/f4\",\"op\":\">\",\"value\":9007199254740992}",
  )
  assert_eq(
    @moonmcap.open_dataset([provider.filter_json(filter)])
    .cursor(@moonmcap.all_query())
    .next_page(10)
    .length(),
    1,
  )
  // Same schema ID and name, different descriptor: field 4 changes from uint64 to string.
  let changed = @moonmcap.decode_hex(
    "0a20120974656c656d6574727922130a0653616d706c6512090a0178180420012809",
  )
  let other = @moonmcap.new_builder()
  let oid = other.register_schema("telemetry.Sample", "protobuf", changed)
  let cid = other.register_channel("/other", "protobuf", schema_id=oid)
  other.append(cid, 0UL, b"\x22\x01z")
  let source = @moonmcap.build_query_index(other.snapshot())
    .query_source()
    .decode_payload(decoder)
  let combined = @moonmcap.open_dataset([source, provider])
  guard combined.cursor(@moonmcap.all_query()).next_record() is Some(record) else {
    fail("missing decoded message")
  }
  assert_eq(
    @json.parse(@utf8.decode(record.message.data[:])),
    @json.parse("{\"x\":\"z\"}"),
  )
  assert_eq(dataset.cursor(@moonmcap.all_query()).next_page(10).length(), 2)
}
