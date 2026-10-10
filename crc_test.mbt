///|
test "IEEE CRC32 published check vector" {
  assert_eq(@moonmcap.crc32(b"123456789"), 0xcbf43926U)
  assert_eq(@moonmcap.crc32(b""), 0U)
  assert_eq(@moonmcap.crc32_extend(@moonmcap.crc32(b"1234"), b"56789"), 0xcbf43926U)
}

///|
test "CRC omission is accepted but a corrupt payload is rejected" {
  @moonmcap.verify_crc(b"changed", 0U)
  assert_true((try? @moonmcap.verify_crc(b"changed", @moonmcap.crc32(b"original"))) is Err(_))
}
