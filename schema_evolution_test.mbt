///|
fn evolution_schema(encoding : String, data : Bytes) -> @moonmcap.Schema {
  {
    id: 1,
    name: if encoding == "protobuf" {
      "p.M"
    } else {
      "demo/Value"
    },
    encoding,
    data,
  }
}

///|
fn evolution_field(
  name : String,
  number : Int32,
  kind : Int32,
  label : Int32,
) -> Bytes {
  b"\x0a" +
  [name.length().to_byte()] +
  @utf8.encode(name[:]) +
  b"\x18" +
  [number.to_byte()] +
  b"\x20" +
  [label.to_byte()] +
  b"\x28" +
  [kind.to_byte()]
}

///|
fn evolution_descriptor(fields : Array[Bytes], proto3~ : Bool = true) -> Bytes {
  let mut message = b"\x0a\x01M"
  for field in fields {
    message += b"\x12" + [field.length().to_byte()] + field
  }
  let file = b"\x12\x01p\x22" +
    [message.length().to_byte()] +
    message +
    (if proto3 { b"\x62\x06proto3" } else { b"\x62\x06proto2" })
  b"\x0a" + [file.length().to_byte()] + file
}

///|
fn evolution_assert(
  report : Json,
  old_reads_new : Bool,
  new_reads_old : Bool,
  queries : Bool,
) -> Unit raise {
  guard report is Object(fields) else { fail("invalid comparison report") }
  assert_eq(fields["old_reader_new_data"], Json::boolean(old_reads_new))
  assert_eq(fields["new_reader_old_data"], Json::boolean(new_reads_old))
  assert_eq(fields["old_query_new_data"], Json::boolean(queries))
}

///|
test "schema comparisons distinguish tagged Protobuf changes and directional required fields" {
  let field = evolution_field("x", 1, 5, 1)
  let old = evolution_schema("protobuf", evolution_descriptor([field]))
  let added = evolution_schema(
    "protobuf",
    evolution_descriptor([field, evolution_field("y", 2, 9, 1)]),
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(old, added),
    true,
    true,
    true,
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(
      old,
      evolution_schema(
        "protobuf",
        evolution_descriptor([evolution_field("value", 1, 5, 1)]),
      ),
    ),
    true,
    true,
    false,
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(
      old,
      evolution_schema(
        "protobuf",
        evolution_descriptor([evolution_field("x", 1, 13, 1)]),
      ),
    ),
    false,
    false,
    false,
  )
  let old = evolution_schema(
    "protobuf",
    evolution_descriptor([field], proto3=false),
  )
  let required = evolution_schema(
    "protobuf",
    evolution_descriptor([field, evolution_field("y", 2, 9, 2)], proto3=false),
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(old, required),
    true,
    false,
    true,
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(required, old),
    false,
    true,
    false,
  )
}

///|
test "ordered ROS layouts and bounds have independent compatibility directions" {
  let old = evolution_schema("ros2msg", b"uint32 x\nstring<=4 label")
  evolution_assert(
    @moonmcap.compare_payload_schemas(
      old,
      evolution_schema("ros2msg", b"uint32 value\nstring<=4 label"),
    ),
    true,
    true,
    false,
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(
      old,
      evolution_schema("ros2msg", b"uint32 x\nstring<=8 label"),
    ),
    false,
    true,
    true,
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(
      old,
      evolution_schema("ros2msg", b"uint32 x\nstring<=4 label\nuint32 extra"),
    ),
    false,
    false,
    false,
  )
  evolution_assert(
    @moonmcap.compare_payload_schemas(
      evolution_schema("ros1msg", b"byte x"),
      evolution_schema("ros1msg", b"int8 x"),
    ),
    true,
    true,
    true,
  )
  let clipped = @moonmcap.compare_payload_schemas(
    old,
    evolution_schema("ros2msg", b"string changed"),
    issue_limit=0,
  )
  assert_true(
    clipped is Object(fields) &&
    fields["truncated"] == Json::boolean(true) &&
    fields["issues"] == Json::array([]),
  )
  evolution_assert(clipped, false, false, false)
}

///|
test "normalized views project aliases explicit units defaults and exact integers" {
  let fields : Array[@moonmcap.NormalizedField] = [
    {
      ..@moonmcap.normalized_field("/speed_mm_s", "/motion/speed_m_s"),
      conversion: @moonmcap.ScaleValue(0.001, 0.0),
    },
    {
      ..@moonmcap.normalized_field("/counter", "/counter"),
      conversion: @moonmcap.UnsignedValue(64),
    },
    {
      ..@moonmcap.normalized_field("/frame", "/frame_id"),
      default_value: Some(Json::string("base_link")),
      conversion: @moonmcap.TextValue,
    },
    {
      ..@moonmcap.normalized_field("/temperature", "/temperature"),
      required: false,
    },
  ]
  let normalized = @moonmcap.payload_normalizer(fields).normalize(
    @json.parse("{\"speed_mm_s\":1250,\"counter\":\"18446744073709551615\"}"),
  )
  assert_eq(
    normalized.value,
    @json.parse(
      "{\"motion\":{\"speed_m_s\":1.25},\"counter\":18446744073709551615,\"frame_id\":\"base_link\"}",
    ),
  )
  assert_eq(normalized.defaulted, ["/frame_id"])
  assert_eq(normalized.missing, ["/temperature"])
  let decoder = @moonmcap.normalizing_decoder(
    @moonmcap.standard_payload_decoder(),
    fields,
  )
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/sample", "json")
  builder.append(
    id, 0UL, b"{\"speed_mm_s\":1250,\"counter\":\"18446744073709551615\"}",
  )
  let stream = @moonmcap.build_query_index(builder.snapshot())
    .query_source()
    .decode_payload(decoder)
    .stream()
  assert_true(
    stream.next() is Some(message) &&
    @json.parse(@utf8.decode(message.data[:])[:]) == normalized.value,
  )
  stream.cancel()
}

///|
test "normalization rejects required missing fields collisions unsafe narrowing and float rounding" {
  let required = @moonmcap.normalized_field("/missing", "/result")
  try @moonmcap.normalize_payload(@json.parse("{}"), [required]) catch {
    _ => ()
  } noraise {
    _ => fail("missing required input accepted")
  }
  try
    @moonmcap.payload_normalizer([
      required,
      { ..required, target: "/result/child", },
    ])
  catch {
    _ => ()
  } noraise {
    _ => fail("overlapping targets accepted")
  }
  for
    conversion in [
      @moonmcap.UnsignedValue(8),
      @moonmcap.SignedValue(8),
      @moonmcap.ScaleValue(1.0, 0.0),
    ] {
    let value = if conversion is @moonmcap.ScaleValue(_, _) {
      @json.parse("9007199254740993")
    } else {
      Json::number(256)
    }
    try
      @moonmcap.normalize_payload(value, [
        { ..@moonmcap.normalized_field("", "/result"), conversion, },
      ])
    catch {
      _ => ()
    } noraise {
      _ => fail("unsafe numeric conversion accepted")
    }
  }
}
