// Derived from bikallem/blit 0.2.2, Apache-2.0.
// Adapted into a portable internal package; see third_party/README.md.

///|
/// Copy `length` bytes from `src` (BytesView) at `src_offset` into `dst` (FixedArray) at `dst_offset`.
/// Note: `src_offset` is relative to the underlying Bytes, not the view's start.
pub fn blit_bytesview(
  dst : FixedArray[Byte],
  dst_offset : Int32,
  src : BytesView,
  src_offset : Int32,
  length : Int32,
) -> Unit {
  blit_bytes(dst, dst_offset, src.data(), src_offset, length)
}

///|
/// Word-at-a-time match length comparison on BytesView.
/// Returns the number of matching bytes between src[a..] and src[b..], up to max_len.
pub fn match_length_bv(
  src : BytesView,
  a : Int32,
  b : Int32,
  max_len : Int32,
) -> Int32 {
  match_length_bytes(src.data(), a, src.data(), b, max_len)
}
