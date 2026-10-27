// Regression tests for adapted runtime components, Apache-2.0.

///|
test "portable blit preserves overlapping ranges and growing buffers" {
  let bytes : FixedArray[Byte] = [0x01, 0x02, 0x03, 0x04, 0x05]
  @blit.blit_fixed_array(bytes, 1, bytes, 0, 4)
  assert_eq(bytes, [0x01, 0x01, 0x02, 0x03, 0x04])
  let buffer = @blit.Buffer::new(size_hint=1)
  buffer.write_byte(0x07)
  buffer.write_byte(0x08)
  buffer.ensure_capacity(1024)
  assert_eq(buffer.to_bytes(), b"\x07\x08")
}
