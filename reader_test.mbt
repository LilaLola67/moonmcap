///|
fn raw_file(parts : Array[Bytes]) -> Bytes {
  let mut bytes = @moonmcap.magic()
  for part in parts { bytes = bytes + part }
  bytes + @moonmcap.magic()
}

///|
test "minimal file is decoded without assuming messages" {
  let bytes = raw_file([
    @moonmcap.frame(0x01, @moonmcap.encode_header({ profile: "", library: "independent" })),
    @moonmcap.frame(0x0f, b"\x00\x00\x00\x00"),
    @moonmcap.frame(0x02, @moonmcap.encode_footer({ summary_start: 0UL, summary_offset_start: 0UL, summary_crc: 0U })),
  ])
  assert_eq(@moonmcap.read_log(bytes).header.library, "independent")
  assert_eq(@moonmcap.scan_file(bytes).length(), 3)
  assert_true((try? @moonmcap.read_log(bytes[0:bytes.length()-1].to_owned())) is Err(_))
}

///|
test "message cannot appear before declaration" {
  let bytes = raw_file([
    @moonmcap.frame(0x01, @moonmcap.encode_header({ profile: "", library: "test" })),
    @moonmcap.frame(0x05, @moonmcap.encode_message({channel_id: 2, sequence: 0U, log_time: 0UL, publish_time: 0UL, data: b""})),
    @moonmcap.frame(0x0f, b"\x00\x00\x00\x00"),
    @moonmcap.frame(0x02, @moonmcap.encode_footer({ summary_start: 0UL, summary_offset_start: 0UL, summary_crc: 0U })),
  ])
  assert_true((try? @moonmcap.read_log(bytes)) is Err(_))
}
