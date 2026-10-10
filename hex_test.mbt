///|
test "hex transport is exhaustive and bounded" {
  let bytes = Bytes::from_array(Array::makei(256, fn(i) { i.to_byte() }))
  assert_eq(@moonmcap.decode_hex(@moonmcap.encode_hex(bytes)), bytes)
  assert_eq(@moonmcap.decode_hex("aAFF"), b"\xaa\xff")
  for invalid in ["a", "zz", "中文"] {
    let rejected = try @moonmcap.decode_hex(invalid) catch {
      _ => true
    } noraise {
      _ => false
    }
    assert_true(rejected)
  }
  let rejected = try @moonmcap.decode_hex("0011", max_bytes=1) catch {
    _ => true
  } noraise {
    _ => false
  }
  assert_true(rejected)
}
