// Derived from bikallem/blit 0.2.2, Apache-2.0.
// Adapted into a portable internal package; see third_party/README.md.

///|
/// Copy `length` bytes between FixedArray[Byte] buffers.
/// Safe for overlapping regions within the same array.
pub fn blit_fixed_array(
  dst : FixedArray[Byte],
  dst_offset : Int32,
  src : FixedArray[Byte],
  src_offset : Int32,
  length : Int32,
) -> Unit {
  FixedArray::unsafe_blit(dst, dst_offset, src, src_offset, length)
}

///|
/// Copy `length` bytes from `src` (Bytes) at `src_offset` into `dst` (FixedArray) at `dst_offset`.
pub fn blit_bytes(
  dst : FixedArray[Byte],
  dst_offset : Int32,
  src : Bytes,
  src_offset : Int32,
  length : Int32,
) -> Unit {
  dst.blit_from_bytes(dst_offset, src, src_offset, length)
}

///|
/// Fill `length` bytes in `dst` starting at `dst_offset` with `value`.
pub fn fill_bytes(
  dst : FixedArray[Byte],
  dst_offset : Int32,
  value : Byte,
  length : Int32,
) -> Unit {
  for i in 0..<length {
    dst[dst_offset + i] = value
  }
}

///|
/// Word-at-a-time match length comparison (byte-by-byte fallback for non-native).
pub fn match_length(
  a : FixedArray[Byte],
  a_offset : Int32,
  b : FixedArray[Byte],
  b_offset : Int32,
  max_len : Int32,
) -> Int32 {
  let mut i = 0
  while i < max_len && a[a_offset + i] == b[b_offset + i] {
    i += 1
  }
  i
}

///|
/// Word-at-a-time match length comparison on Bytes (byte-by-byte fallback for non-native).
pub fn match_length_bytes(
  a : Bytes,
  a_offset : Int32,
  b : Bytes,
  b_offset : Int32,
  max_len : Int32,
) -> Int32 {
  let mut i = 0
  while i < max_len && a[a_offset + i] == b[b_offset + i] {
    i += 1
  }
  i
}

///|
/// Subtract `delta` from each of `len` elements, clamping to `floor`.
pub fn shift_int_array(
  arr : FixedArray[Int32],
  len : Int32,
  delta : Int32,
  floor : Int32,
) -> Unit {
  for i in 0..<len {
    let v = arr[i] - delta
    arr[i] = if v < floor { floor } else { v }
  }
}

///|
/// Allocate a FixedArray[Byte] without zeroing memory.
/// The caller MUST fully initialize the buffer before reading from it.
pub fn make_uninit(len : Int32) -> FixedArray[Byte] {
  FixedArray::make(len, b'\x00')
}

///|
pub fn make_uninit_int(len : Int32) -> FixedArray[Int32] {
  FixedArray::make(len, 0)
}

///|
pub fn make_uninit_uint(len : Int32) -> FixedArray[UInt32] {
  FixedArray::make(len, 0U)
}
