///|
test "message wire matches independent little endian bytes" {
  let message : @moonmcap.Message = {
    channel_id: 7,
    sequence: 0x12345678U,
    log_time: 1UL,
    publish_time: 2UL,
    data: b"ok",
  }
  let expected = b"\x07\x00\x78\x56\x34\x12\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00ok"
  assert_eq(@moonmcap.encode_message(message), expected)
  assert_eq(@moonmcap.decode_message(expected), message)
}

///|
test "unicode header and metadata round trip" {
  let header : @moonmcap.Header = { profile: "", library: "月亮日志", }
  assert_eq(@moonmcap.decode_header(@moonmcap.encode_header(header)), header)
  let metadata : @moonmcap.Metadata = {
    name: "run",
    entries: [("地点", "实验室"), ("id", "42")],
  }
  assert_eq(
    @moonmcap.decode_metadata(@moonmcap.encode_metadata(metadata)),
    metadata,
  )
}

///|
test "schemas and channels retain identifiers and payloads" {
  let schema : @moonmcap.Schema = {
    id: 65535,
    name: "T",
    encoding: "jsonschema",
    data: b"{}",
  }
  assert_eq(@moonmcap.decode_schema(@moonmcap.encode_schema(schema)), schema)
  let channel : @moonmcap.Channel = {
    id: 0,
    schema_id: 65535,
    topic: "/temperature",
    message_encoding: "json",
    metadata: [],
  }
  assert_eq(
    @moonmcap.decode_channel(@moonmcap.encode_channel(channel)),
    channel,
  )
}

///|
test "malformed UTF8 duplicate keys and footer extension are rejected" {
  assert_true(
    (Result::Ok(@moonmcap.decode_header(b"\x01\x00\x00\x00\xff")) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
  let bad : @moonmcap.Metadata = {
    name: "x",
    entries: [("a", "1"), ("a", "2")],
  }
  assert_true(
    (Result::Ok(@moonmcap.encode_metadata(bad)) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
  let footer : @moonmcap.Footer = {
    summary_start: 0UL,
    summary_offset_start: 0UL,
    summary_crc: 0U,
  }
  assert_true(
    (Result::Ok(@moonmcap.decode_footer(@moonmcap.encode_footer(footer) + b"x")) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
}
