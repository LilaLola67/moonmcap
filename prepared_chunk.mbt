///|
/// Verify a worker response against the original wire metadata and nested limits.
fn decode_prepared_chunk(
  original : Bytes,
  expanded : Bytes,
  limits : Limits,
) -> Chunk raise McapError {
  if limits.max_decompressed_bytes > 2147483607 ||
    expanded.length() > limits.max_decompressed_bytes + 40 {
    raise Invalid("expanded worker Chunk exceeds budget")
  }
  let wire = cursor(original, limits)
  let times = wire.take(16)
  let size = wire.u64()
  let checksum = wire.u32()
  ignore(wire.text())
  let bytes = wire.u64()
  if bytes != (original.length() - wire.pos).to_uint64() {
    raise Invalid("original Chunk length mismatch")
  }
  let metadata = cursor(expanded, {
    ..limits,
    max_record_bytes: limits.max_decompressed_bytes + 40,
  })
  if metadata.take(16) != times ||
    metadata.u64() != size ||
    metadata.u32() != checksum ||
    metadata.text() != "" ||
    metadata.u64() != size {
    raise Invalid("expanded Chunk differs from original properties")
  }
  decode_chunk_internal(expanded, limits, limits.max_decompressed_bytes + 40)
}
