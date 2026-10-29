///|
/// None removes an object member or array element. Set replaces the addressed
/// value, or adds a final object member. Parent containers must already exist.
pub(all) struct PayloadEdit {
  path : String
  replacement : Json?
}

///|
fn payload_edit_at(
  document : Json,
  path : Array[String],
  position : Int,
  replacement : Json?,
) -> Json raise McapError {
  if position == path.length() {
    return match replacement {
      Some(value) => value
      None => raise Invalid("cannot remove payload root")
    }
  }
  let key = path[position]
  let last = position + 1 == path.length()
  match document {
    Object(fields) => {
      let copy : Map[String, Json] = Map([])
      for name in fields.keys() {
        copy[name] = fields[name]
      }
      if last {
        match replacement {
          Some(value) => copy[key] = value
          None => {
            if !copy.contains(key) {
              raise Invalid("payload removal path absent")
            }
            copy.remove(key) |> ignore
          }
        }
      } else {
        guard fields.get(key) is Some(child) else {
          raise Invalid("payload parent path absent")
        }
        copy[key] = payload_edit_at(child, path, position + 1, replacement)
      }
      Json::object(copy)
    }
    Array(items) => {
      let index = parse_decimal(key)
      if index >= items.length().to_uint64() || index.to_string() != key {
        raise Invalid("payload array index invalid")
      }
      let copy = items.copy()
      if last && replacement is None {
        copy.remove(index.to_int()) |> ignore
      } else {
        copy[index.to_int()] = payload_edit_at(
          items[index.to_int()],
          path,
          position + 1,
          replacement,
        )
      }
      Json::array(copy)
    }
    _ => raise Invalid("payload edit traverses a scalar")
  }
}

///|
fn payload_edit_paths(
  edits : Array[PayloadEdit],
) -> Array[Array[String]] raise McapError {
  if edits.length() > 128 {
    raise Invalid("payload edit count exceeds 128")
  }
  let paths = edits.map(fn(e) { json_pointer(e.path) })
  for i = 0; i < paths.length(); i = i + 1 {
    for j = 0; j < i; j = j + 1 {
      let a = paths[i]
      let b = paths[j]
      let mut common = true
      for k = 0; k < a.length() && k < b.length(); k = k + 1 {
        if a[k] != b[k] {
          common = false
          break
        }
      }
      if common {
        raise Invalid("overlapping payload edits")
      }
    }
  }
  paths
}

///|
pub fn edit_payload_json(
  document : Json,
  edits : Array[PayloadEdit],
) -> Json raise McapError {
  let paths = payload_edit_paths(edits)
  let mut result = document
  // Array removals affect subsequent indices; their order is deliberately the
  // caller's explicit edit order, with each path resolved against that result.
  for i = 0; i < edits.length(); i = i + 1 {
    result = payload_edit_at(result, paths[i], 0, edits[i].replacement)
  }
  result
}

///|
fn payload_unsigned(value : Json, bits : Int) -> UInt64 raise McapError {
  let text = match value {
    String(text) => text
    Number(_, ..) => value.stringify()
    _ => raise Invalid("payload unsigned integer expected")
  }
  let result = parse_decimal(text)
  if bits < 64 && result >= 1UL << bits {
    raise Invalid("payload unsigned integer out of range")
  }
  result
}

///|
fn payload_signed(value : Json, bits : Int) -> UInt64 raise McapError {
  let number = proto_integer(value)
  if bits < 64 && (number < -(1L << (bits - 1)) || number >= 1L << (bits - 1)) {
    raise Invalid("payload signed integer out of range")
  }
  number.to_uint64()
}

///|
fn payload_float(value : Json) -> Double raise McapError {
  match value {
    Number(number, ..) if !number.is_nan() && !number.is_inf() => number
    String("NaN") => Double::nan()
    String("Infinity") => Double::inf(1)
    String("-Infinity") => Double::inf(-1)
    _ => raise Invalid("payload floating point value expected")
  }
}

///|
struct PayloadOutput {
  buffer : @buffer.Buffer
  maximum : Int
  budget : Array[Int]
  little : Bool
  aligned : Bool
}

///|
fn PayloadOutput::write(
  self : PayloadOutput,
  bytes : Bytes,
) -> Unit raise McapError {
  if bytes.length() > self.maximum - self.buffer.length() {
    raise Invalid("encoded payload exceeds byte budget")
  }
  self.buffer.write_bytes(bytes[:])
}

///|
fn PayloadOutput::item(self : PayloadOutput) -> Unit raise McapError {
  if self.budget[0] <= 0 {
    raise Invalid("payload encoding item budget exceeded")
  }
  self.budget[0] -= 1
}

///|
fn PayloadOutput::unsigned(
  self : PayloadOutput,
  number : UInt64,
  width : Int,
) -> Unit raise McapError {
  let padding = if self.aligned {
    (width - (self.buffer.length() - 4) % width) % width
  } else {
    0
  }
  self.write(Bytes::make(padding, 0x00))
  let bytes = Bytes::makei(width, fn(i) {
    (number >> (8 * (if self.little { i } else { width - i - 1 }))).to_byte()
  })
  self.write(bytes)
}

///|
fn CdrDecoder::encode_value(
  self : CdrDecoder,
  wire : PayloadOutput,
  field : CdrField,
  value : Json,
  depth : Int,
) -> Unit raise McapError {
  wire.item()
  match field.base {
    "bool" =>
      match value {
        True => wire.unsigned(1UL, 1)
        False => wire.unsigned(0UL, 1)
        _ => raise Invalid("payload boolean expected")
      }
    "byte" | "char" | "uint8" => wire.unsigned(payload_unsigned(value, 8), 1)
    "int8" => wire.unsigned(payload_signed(value, 8), 1)
    "uint16" => wire.unsigned(payload_unsigned(value, 16), 2)
    "int16" => wire.unsigned(payload_signed(value, 16), 2)
    "uint32" => wire.unsigned(payload_unsigned(value, 32), 4)
    "int32" => wire.unsigned(payload_signed(value, 32), 4)
    "uint64" => wire.unsigned(payload_unsigned(value, 64), 8)
    "int64" => wire.unsigned(payload_signed(value, 64), 8)
    "float64" => wire.unsigned(payload_float(value).reinterpret_as_uint64(), 8)
    "float32" => {
      let number = payload_float(value)
      let converted = number.to_float()
      if !number.is_inf() && converted.to_double().is_inf() {
        raise Invalid("payload float32 overflow")
      }
      wire.unsigned(converted.reinterpret_as_uint().to_uint64(), 4)
    }
    "string" => {
      guard value is String(text) else {
        raise Invalid("payload string expected")
      }
      let bytes = @utf8.encode(text[:])
      if bytes.length() > self.options.string_bytes ||
        (field.string_bound is Some(bound) && bytes.length() > bound) ||
        (!self.ros1 && bytes.iter().any(fn(b) { b == 0x00 })) {
        raise Invalid("payload string exceeds bound or contains CDR NUL")
      }
      wire.unsigned(
        (bytes.length() + (if self.ros1 { 0 } else { 1 })).to_uint64(),
        4,
      )
      wire.write(bytes)
      if !self.ros1 {
        wire.write(b"\x00")
      }
    }
    _ => self.encode_message(wire, field.base, value, depth + 1)
  }
}

///|
fn CdrDecoder::encode_message(
  self : CdrDecoder,
  wire : PayloadOutput,
  name : String,
  value : Json,
  depth : Int,
) -> Unit raise McapError {
  if depth > self.options.depth {
    raise Invalid("payload encoding depth exceeded")
  }
  guard value is Object(values) else {
    raise Invalid("payload message object expected")
  }
  let fields = self.definitions[name]
  for key in values.keys() {
    if !fields.any(fn(f) { f.name == key }) {
      raise Invalid("unknown payload field: " + key)
    }
  }
  if self.ros1 {
    validate_ros1_time(name, values)
  }
  if fields.is_empty() && !self.ros1 {
    wire.item()
    wire.unsigned(0UL, 1)
  }
  for field in fields {
    guard values.get(field.name) is Some(value) else {
      raise Invalid("required payload field absent: " + field.name)
    }
    match field.array {
      None => self.encode_value(wire, field, value, depth)
      Some(size) => {
        wire.item()
        guard value is Array(items) else {
          raise Invalid("payload array expected")
        }
        if items.length() > wire.budget[0] ||
          (size >= 0 && items.length() != size) ||
          (field.bound is Some(bound) && items.length() > bound) {
          raise Invalid("payload array size or budget exceeded")
        }
        if size < 0 {
          wire.unsigned(items.length().to_uint64(), 4)
        }
        for item in items {
          self.encode_value(wire, field, item, depth)
        }
      }
    }
  }
}

///|
pub fn CdrDecoder::encode(
  self : CdrDecoder,
  value : Json,
  little_endian? : Bool = true,
) -> Bytes raise McapError {
  let wire : PayloadOutput = {
    buffer: @buffer.Buffer::Buffer(),
    maximum: self.options.message_bytes,
    budget: [self.options.items],
    little: little_endian,
    aligned: true,
  }
  wire.write(
    if little_endian {
      b"\x00\x01\x00\x00"
    } else {
      b"\x00\x00\x00\x00"
    },
  )
  self.encode_message(wire, self.name, value, 1)
  let bytes = wire.buffer.to_bytes()
  // Apply the same payload validation as the decoder before returning bytes.
  ignore(self.decode(bytes))
  bytes
}

///|
pub fn Ros1Decoder::encode(
  self : Ros1Decoder,
  value : Json,
) -> Bytes raise McapError {
  let wire : PayloadOutput = {
    buffer: @buffer.Buffer::Buffer(),
    maximum: self.layout.options.message_bytes,
    budget: [self.layout.options.items],
    little: true,
    aligned: false,
  }
  self.layout.encode_message(wire, self.layout.name, value, 1)
  let bytes = wire.buffer.to_bytes()
  ignore(self.decode(bytes))
  bytes
}
