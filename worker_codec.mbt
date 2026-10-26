///|
pub fn decode_chunk(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> Chunk raise McapError {
  decode_chunk_internal(bytes, limits, limits.max_record_bytes)
}

///|
/// Pure MoonBit work unit: bounded decompression and CRC, preserving raw records.
pub fn expand_worker_chunk(
  body : Bytes,
  limits? : Limits = default_limits(),
) -> Bytes raise McapError {
  if body.length() > limits.max_record_bytes ||
    limits.max_decompressed_bytes > 2147483607 {
    raise Invalid("worker Chunk exceeds byte budget")
  }
  let input = cursor(body, limits)
  let start = input.u64()
  let end = input.u64()
  let size = input.u64()
  let checksum = input.u32()
  let compression = input.text()
  let encoded = bounded_length(
    input.u64(),
    limits.max_record_bytes,
    "worker compressed Chunk",
  )
  let payload = decompress_payload(
    input.take(encoded),
    compression,
    size,
    limits~,
  )
  input.finish()
  if start > end {
    raise Invalid("inverted worker Chunk time range")
  }
  verify_crc(payload, checksum, label="worker Chunk")
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(start)
  out.write_uint64_le(end)
  out.write_uint64_le(size)
  out.write_uint_le(checksum)
  out.write_uint_le(0U)
  out.write_uint64_le(size)
  out.write_bytes(payload[:])
  out.to_bytes()
}

///|
pub(all) struct ChunkReadRequest {
  span : ReadSpan
  expanded_bytes : Int
  compression : String
}

///|
pub fn FileCursor::upcoming_reads(
  self : FileCursor,
  maximum : Int,
) -> Array[ChunkReadRequest] raise McapError {
  if maximum < 1 || maximum > 16 {
    raise Invalid("invalid worker prefetch count")
  }
  let requests = []
  if self.state != FileActive || self.fallback is Some(_) {
    return requests
  }
  for i = self.loaded
      i < self.candidates.length() && requests.length() < maximum
      i = i + 1 {
    let index = self.candidates[i]
    if self.reader.cache.any(fn(c) { c.offset == index.chunk_start }) {
      continue
    }
    let total = index.chunk_length + index.message_index_length
    if total > self.span_budget.to_uint64() ||
      index.uncompressed_size >
      self.reader.limits.max_decompressed_bytes.min(2147483607).to_uint64() {
      if !requests.is_empty() {
        break
      }
      raise Invalid("worker read span or expansion exceeds budget")
    }
    let length = total.to_int()
    let expanded = index.uncompressed_size.to_int() + 40
    requests.push({
      span: { offset: index.chunk_start, length, },
      expanded_bytes: expanded,
      compression: index.compression,
    })
  }
  requests
}

///|
/// Host worker must be trusted; original properties, output CRC and indexes are checked.
pub fn FileCursor::feed_expanded(
  self : FileCursor,
  original : Bytes,
  expanded : Bytes,
) -> Unit raise McapError {
  if self.state != FileActive || !self.needed() {
    raise Invalid("file cursor has no pending worker read")
  }
  try {
    let index = self.candidates[self.loaded]
    let length = bounded_length(
      index.chunk_length + index.message_index_length,
      self.span_budget,
      "worker cursor span",
    )
    if original.length() != length {
      raise Invalid("worker original span length mismatch")
    }
    self.accept(
      self.reader.load_chunk(index, provided=Some(original), expanded~),
    )
  } catch {
    error => {
      self.fail()
      raise error
    }
  }
}

///|
pub fn RangeReader::declaration_log(self : RangeReader) -> Log {
  copy_log(self.declarations)
}
