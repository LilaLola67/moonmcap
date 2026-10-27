///|
// Keep the wire primitive independent of removed standard-library methods.
fn UInt::mcap_le_bytes(self : UInt) -> Bytes {
  [
    self.to_byte(),
    (self >> 8).to_byte(),
    (self >> 16).to_byte(),
    (self >> 24).to_byte(),
  ]
}

///|
struct Cursor {
  data : Bytes
  mut pos : Int
  limits : Limits
}

///|
fn cursor(data : Bytes, limits : Limits) -> Cursor {
  { data, pos: 0, limits, }
}

///|
fn Cursor::take(self : Cursor, count : Int) -> Bytes raise McapError {
  if count < 0 || count > self.data.length() - self.pos {
    raise Invalid("truncated field at byte \{self.pos}: requested \{count}")
  }
  let result = self.data[self.pos:self.pos + count].to_owned()
  self.pos += count
  result
}

///|
fn Cursor::u16(self : Cursor) -> Int raise McapError {
  let bytes = self.take(2)
  bytes[0].to_int() | (bytes[1].to_int() << 8)
}

///|
fn Cursor::u32(self : Cursor) -> UInt raise McapError {
  let bytes = self.take(4)
  let mut result = 0U
  for i = 0; i < 4; i = i + 1 {
    result = result | (bytes[i].to_uint() << (i * 8))
  }
  result
}

///|
fn Cursor::u64(self : Cursor) -> UInt64 raise McapError {
  let bytes = self.take(8)
  let mut result = 0UL
  for i = 0; i < 8; i = i + 1 {
    result = result | (bytes[i].to_uint64() << (i * 8))
  }
  result
}

///|
fn bounded_length(
  value : UInt64,
  maximum : Int,
  label : String,
) -> Int raise McapError {
  if maximum < 0 || value > maximum.to_uint64() {
    raise Invalid("\{label} length exceeds configured limit")
  }
  value.to_int()
}

///|
fn Cursor::text(self : Cursor) -> String raise McapError {
  let size = bounded_length(
    self.u32().to_uint64(),
    self.limits.max_string_bytes,
    "string",
  )
  let bytes = self.take(size)
  @utf8.decode(bytes[:], ignore_bom=true) catch {
    _ => raise Invalid("malformed UTF-8 at byte \{self.pos - size}")
  }
}

///|
fn Cursor::blob(self : Cursor) -> Bytes raise McapError {
  let size = bounded_length(
    self.u32().to_uint64(),
    self.limits.max_record_bytes,
    "blob",
  )
  self.take(size)
}

///|
fn Cursor::pairs(self : Cursor) -> Array[(String, String)] raise McapError {
  let size = bounded_length(
    self.u32().to_uint64(),
    self.limits.max_record_bytes,
    "map",
  )
  let nested = cursor(self.take(size), self.limits)
  let pairs = []
  let seen : Map[String, Bool] = {}
  while nested.pos < nested.data.length() {
    let key = nested.text()
    let value = nested.text()
    if seen.contains(key) {
      raise Invalid("duplicate map key: \{key}")
    }
    seen[key] = true
    pairs.push((key, value))
  }
  pairs
}

///|
fn Cursor::finish(self : Cursor) -> Unit raise McapError {
  if self.pos != self.data.length() {
    raise Invalid("unexpected trailing bytes at \{self.pos}")
  }
}

///|
fn put_u16(out : @buffer.Buffer, value : Int) -> Unit raise McapError {
  if value < 0 || value > 65535 {
    raise Invalid("identifier outside uint16")
  }
  out.write_uint16_le(value.to_uint16())
}

///|
fn put_text(out : @buffer.Buffer, value : String) -> Unit {
  let bytes = @utf8.encode(value[:])
  out.write_uint_le(bytes.length().to_uint())
  out.write_bytes(bytes[:])
}

///|
fn put_blob(out : @buffer.Buffer, value : Bytes) -> Unit {
  out.write_uint_le(value.length().to_uint())
  out.write_bytes(value[:])
}

///|
fn put_pairs(
  out : @buffer.Buffer,
  pairs : Array[(String, String)],
) -> Unit raise McapError {
  let nested = @buffer.Buffer::Buffer()
  let seen : Map[String, Bool] = {}
  for pair in pairs {
    if seen.contains(pair.0) {
      raise Invalid("duplicate map key: \{pair.0}")
    }
    seen[pair.0] = true
    put_text(nested, pair.0)
    put_text(nested, pair.1)
  }
  put_blob(out, nested.to_bytes())
}

///|
pub fn frame(opcode : Byte, body : Bytes) -> Bytes raise McapError {
  if opcode == 0 {
    raise Invalid("opcode zero is reserved")
  }
  let out = @buffer.Buffer::Buffer()
  out.write_byte(opcode)
  out.write_uint64_le(body.length().to_uint64())
  out.write_bytes(body[:])
  out.to_bytes()
}

///|
pub fn decode_records(
  bytes : Bytes,
  limits? : Limits = default_limits(),
  base? : Int = 0,
) -> Array[Record] raise McapError {
  if limits.max_records < 0 || base < 0 || base > 2147483647 - bytes.length() {
    raise Invalid("invalid scan limits or base")
  }
  let input = cursor(bytes, limits)
  let result = []
  while input.pos < bytes.length() {
    if result.length() >= limits.max_records {
      raise Invalid("record count exceeds configured limit")
    }
    let offset = input.pos
    let opcode = input.take(1)[0]
    if opcode == 0 {
      raise Invalid("opcode zero at byte \{offset + base}")
    }
    let size = bounded_length(input.u64(), limits.max_record_bytes, "record")
    let body = input.take(size)
    result.push({ opcode, offset: offset + base, body, })
  }
  result
}
