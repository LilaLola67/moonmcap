///|
/// Incremental record framing. File magic/envelope checks belong to scan_file.
pub struct StreamDecoder {
  limits : Limits
  pending : @buffer.Buffer
  mut expected : Int
  mut offset : Int
  mut count : Int
  mut failed : Bool
  mut finished : Bool
}

///|
pub fn StreamDecoder::new(limits? : Limits = default_limits(), base? : Int = 0) -> StreamDecoder raise McapError {
  if limits.max_record_bytes < 0 || limits.max_records < 0 || base < 0 {
    raise Invalid("invalid stream limits")
  }
  {limits, pending: @buffer.Buffer::Buffer(), expected: -1, offset: base, count: 0, failed: false, finished: false}
}

///|
pub fn StreamDecoder::buffered_bytes(self : StreamDecoder) -> Int { self.pending.length() }

///|
pub fn StreamDecoder::record_count(self : StreamDecoder) -> Int { self.count }

///|
pub fn StreamDecoder::feed(self : StreamDecoder, bytes : Bytes) -> Array[Record] raise McapError {
  if self.failed || self.finished { raise Invalid("stream is closed or poisoned") }
  try self.feed_inner(bytes) catch {
    error => {
      self.failed = true
      self.pending.reset()
      raise error
    }
  }
}

///|
fn StreamDecoder::feed_inner(self : StreamDecoder, bytes : Bytes) -> Array[Record] raise McapError {
  let result = []
  let mut position = 0
  while position < bytes.length() {
    if self.expected == -1 {
      let missing = 9 - self.pending.length()
      let count = missing.min(bytes.length() - position)
      self.pending.write_bytes(bytes[position:position+count])
      position += count
      if self.pending.length() < 9 { continue }
      if self.count >= self.limits.max_records { raise Invalid("stream record budget exceeded") }
      let header = self.pending.to_bytes()
      if header[0] == 0 { raise Invalid("reserved stream opcode") }
      let input = cursor(header[1:].to_owned(), self.limits)
      self.expected = bounded_length(input.u64(), self.limits.max_record_bytes, "stream record")
      if self.expected > 2147483647 - 9 { raise Invalid("stream length arithmetic overflow") }
    }
    let missing = self.expected + 9 - self.pending.length()
    let count = missing.min(bytes.length() - position)
    self.pending.write_bytes(bytes[position:position+count])
    position += count
    if self.pending.length() == self.expected + 9 {
      let complete = self.pending.to_bytes()
      if self.offset > 2147483647 - complete.length() { raise Invalid("stream offset arithmetic overflow") }
      result.push({opcode: complete[0], offset: self.offset, body: complete[9:].to_owned()})
      self.offset += complete.length()
      self.count += 1
      self.pending.reset()
      self.expected = -1
    }
  }
  result
}

///|
pub fn StreamDecoder::finish(self : StreamDecoder) -> Unit raise McapError {
  if self.failed || self.finished { raise Invalid("stream is closed or poisoned") }
  self.finished = true
  if self.pending.length() != 0 {
    self.pending.reset()
    raise Invalid("truncated stream record at end of input")
  }
}
