///|
struct CdrWire {
  bytes : Bytes
  little : Bool
  mut position : Int
  budget : Array[Int]
  aligned : Bool
}

///|
fn CdrWire::take(self : CdrWire, count : Int) -> Bytes raise McapError {
  if count < 0 || count > self.bytes.length() - self.position {
    raise Invalid("truncated XCDR1 payload")
  }
  let out = self.bytes[self.position:self.position + count].to_owned()
  self.position += count
  out
}

///|
fn CdrWire::unsigned(self : CdrWire, width : Int) -> UInt64 raise McapError {
  let padding = if self.aligned {
    (width - (self.position - 4) % width) % width
  } else {
    0
  }
  ignore(self.take(padding))
  let data = self.take(width)
  let mut result = 0UL
  for i = 0; i < width; i = i + 1 {
    result = result |
      (data[if self.little { i } else { width - i - 1 }].to_uint64() << (i * 8))
  }
  result
}

///|
fn CdrWire::item(self : CdrWire) -> Unit raise McapError {
  if self.budget[0] <= 0 {
    raise Invalid("CDR item budget exceeded")
  }
  self.budget[0] -= 1
}

///|
fn CdrDecoder::value(
  self : CdrDecoder,
  wire : CdrWire,
  field : CdrField,
  depth : Int,
) -> Json raise McapError {
  wire.item()
  match field.base {
    "bool" => {
      let value = wire.unsigned(1)
      if value > 1UL {
        raise Invalid("invalid CDR boolean")
      }
      Json::boolean(value == 1UL)
    }
    "byte" | "char" | "uint8" => Json::number(wire.unsigned(1).to_double())
    "int8" => {
      let value = wire.unsigned(1).to_int()
      Json::number((if value >= 128 { value - 256 } else { value }).to_double())
    }
    "uint16" => Json::number(wire.unsigned(2).to_double())
    "int16" => {
      let value = wire.unsigned(2).to_int()
      Json::number(
        (if value >= 32768 { value - 65536 } else { value }).to_double(),
      )
    }
    "uint32" => Json::number(wire.unsigned(4).to_double())
    "int32" => Json::number(wire.unsigned(4).to_uint().to_int().to_double())
    "uint64" | "int64" => {
      let value = wire.unsigned(8)
      let text = if field.base == "int64" {
        value.to_int64().to_string()
      } else {
        value.to_string()
      }
      if self.options.int64_as_string {
        Json::string(text)
      } else {
        Json::number(
          if field.base == "int64" {
            value.to_int64().to_double()
          } else {
            value.to_double()
          },
          repr=text,
        )
      }
    }
    "float64" => protobuf_float(wire.unsigned(8).reinterpret_as_double())
    "float32" => {
      let value = Float::reinterpret_from_uint(wire.unsigned(4).to_uint())
      if value.to_double().is_nan() || value.to_double().is_inf() {
        protobuf_float(value.to_double())
      } else {
        Json::number(value.to_double(), repr=value.to_string())
      }
    }
    "string" => {
      let length = wire.unsigned(4)
      if self.ros1 {
        if length > self.options.string_bytes.to_uint64() {
          raise Invalid("ROS1 string exceeds byte budget")
        }
        return Json::string(
          @utf8.decode(wire.take(length.to_int())[:]) catch {
            _ => raise Invalid("invalid ROS1 string UTF-8")
          },
        )
      }
      if length == 0UL || length - 1UL > self.options.string_bytes.to_uint64() {
        raise Invalid("CDR string length exceeds budget or lacks NUL")
      }
      let bytes = wire.take(length.to_int())
      if bytes[bytes.length() - 1] != 0x00 ||
        bytes[0:bytes.length() - 1].iter().any(fn(b) { b == 0x00 }) {
        raise Invalid("CDR string must have one terminal NUL")
      }
      if field.string_bound is Some(bound) && length - 1UL > bound.to_uint64() {
        raise Invalid("CDR string exceeds declared byte bound")
      }
      Json::string(
        @utf8.decode(bytes[0:bytes.length() - 1]) catch {
          _ => raise Invalid("invalid CDR string UTF-8")
        },
      )
    }
    _ => self.message(wire, field.base, depth + 1)
  }
}

///|
fn CdrDecoder::message(
  self : CdrDecoder,
  wire : CdrWire,
  name : String,
  depth : Int,
) -> Json raise McapError {
  if depth > self.options.depth {
    raise Invalid("CDR message depth budget exceeded")
  }
  let fields = self.definitions[name]
  let result : Map[String, Json] = Map([])
  if fields.is_empty() && !self.ros1 {
    wire.item()
    if wire.unsigned(1) != 0UL {
      raise Invalid("invalid ROS 2 empty message sentinel")
    }
  }
  for field in fields {
    let value = match field.array {
      None => self.value(wire, field, depth)
      Some(array) => {
        wire.item()
        let count = if array < 0 {
          let size = wire.unsigned(4)
          if size > wire.budget[0].to_uint64() {
            raise Invalid("CDR sequence exceeds item budget")
          }
          size.to_int()
        } else {
          array
        }
        if count > wire.budget[0] ||
          (field.bound is Some(bound) && count > bound) {
          raise Invalid("CDR array exceeds item or declared bound")
        }
        let list = []
        for i = 0; i < count; i = i + 1 {
          list.push(self.value(wire, field, depth))
        }
        Json::array(list)
      }
    }
    result[field.name] = value
  }
  if self.ros1 {
    validate_ros1_time(name, result)
  }
  Json::object(result)
}

///|
pub fn CdrDecoder::decode(
  self : CdrDecoder,
  bytes : Bytes,
) -> Json raise McapError {
  if bytes.length() < 4 || bytes.length() > self.options.message_bytes {
    raise Invalid("CDR payload byte budget or short encapsulation")
  }
  if bytes[0] != 0x00 ||
    (bytes[1] != 0x00 && bytes[1] != 0x01) ||
    bytes[2] != 0x00 ||
    bytes[3] != 0x00 {
    raise Invalid(
      "only plain XCDR1 with zero encapsulation options is supported",
    )
  }
  let wire : CdrWire = {
    bytes,
    little: bytes[1] == 0x01,
    position: 4,
    budget: [self.options.items],
    aligned: true,
  }
  let result = self.message(wire, self.name, 1)
  let remaining = bytes.length() - wire.position
  if remaining > 3 || bytes[wire.position:].iter().any(fn(b) { b != 0x00 }) {
    raise Invalid("trailing CDR payload bytes")
  }
  result
}
