///|
test "IEEE CRC32 published check vector" {
  assert_eq(@moonmcap.crc32(b"123456789"), 0xcbf43926U)
  assert_eq(@moonmcap.crc32(b""), 0U)
  assert_eq(
    @moonmcap.crc32_extend(@moonmcap.crc32(b"1234"), b"56789"),
    0xcbf43926U,
  )
}

///|
test "IEEE CRC32 covers every byte value and preserves finalized state across segments" {
  // Reference values from Python zlib.crc32 over bytes(range(256)) * 17.
  let payload = Bytes::makei(4352, i => (i % 256).to_byte())
  assert_eq(@moonmcap.crc32(payload), 0x671a56a6U)
  assert_eq(@moonmcap.crc32_extend(0x89abcdefU, payload), 0xbfd3bbcdU)
  for split in [0, 1, 255, 256, 1025, 4352] {
    let prefix = @moonmcap.crc32_extend(0x89abcdefU, payload[:split].to_owned())
    assert_eq(
      @moonmcap.crc32_extend(prefix, payload[split:].to_owned()),
      0xbfd3bbcdU,
    )
  }
}

///|
test "CRC omission is accepted but a corrupt payload is rejected" {
  @moonmcap.verify_crc(b"changed", 0U)
  assert_true(
    (Result::Ok(@moonmcap.verify_crc(b"changed", @moonmcap.crc32(b"original"))) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
}
