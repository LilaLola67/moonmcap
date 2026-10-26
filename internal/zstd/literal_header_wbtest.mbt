///|
test "size-format-three separates 18-bit literal and compressed sizes" {
  for regenerated in [16384UL, 40000UL, 131072UL, 262143UL] {
    for compressed in [1UL, 30000UL, 131071UL] {
      let bits = 14UL | (regenerated << 4) | (compressed << 22)
      let header = bits.to_le_bytes()[0:5].to_owned()
      let (size, encoded, length, streams) = decode_huffman_literals_header(
        header[:],
        0,
        5,
      )
      assert_eq(size, regenerated.to_int())
      assert_eq(encoded, compressed.to_int())
      assert_eq(length, 5)
      assert_eq(streams, 4)
    }
  }
}
