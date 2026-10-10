///|
test "framing preserves uint64 length and absolute offset" {
  let bytes = @moonmcap.frame(0x80, b"abc")
  assert_eq(bytes[1].to_int(), 3)
  assert_eq(bytes.length(), 12)
  let records = @moonmcap.decode_records(bytes, base=8)
  assert_eq(records[0].opcode, 0x80)
  assert_eq(records[0].offset, 8)
  assert_eq(records[0].body, b"abc")
}

///|
test "truncated headers and huge length fail before allocation" {
  let tiny = @moonmcap.frame(0x80, b"abc")
  let result = try? @moonmcap.decode_records(tiny[0:8].to_owned())
  assert_true(result is Err(_))
  let huge = b"\x80\xff\xff\xff\xff\xff\xff\xff\xff"
  assert_true((try? @moonmcap.decode_records(huge)) is Err(_))
  assert_true((try? @moonmcap.frame(0, b"")) is Err(_))
}

///|
test "record budget is enforced" {
  let limits = @moonmcap.Limits::{
    max_record_bytes: 100,
    max_string_bytes: 50,
    max_records: 1,
  }
  let bytes = @moonmcap.frame(0x80, b"a") + @moonmcap.frame(0x81, b"b")
  assert_true((try? @moonmcap.decode_records(bytes, limits~)) is Err(_))
}
