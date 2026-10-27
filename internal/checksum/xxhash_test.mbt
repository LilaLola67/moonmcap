// Regression tests for adapted runtime components, Apache-2.0.

///|
test "portable xxHash retains official empty and abc vectors" {
  assert_eq(@checksum.xxh32(b""[:]), 0x02cc5d05U)
  assert_eq(@checksum.xxh32(b"abc"[:]), 0x32d153ffU)
  let empty = @checksum.XXH64::new()
  assert_eq(empty.checksum64(), 0xef46db3751d8e999UL)
  let hash = @checksum.XXH32::new()
  hash.update(b"a"[:])
  hash.update(b"bc"[:])
  assert_eq(hash.checksum(), 0x32d153ffU)
  let hash = @checksum.XXH64::new()
  hash.update(b"a"[:])
  hash.update(b"bc"[:])
  assert_eq(hash.checksum64(), 0x44bc2cf5ad770999UL)
}
