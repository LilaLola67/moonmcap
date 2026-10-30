///|
pub(all) struct ProtobufOptions {
  descriptor_bytes : Int32
  message_bytes : Int32
  depth : Int32
  items : Int32
  fields : Int32
  int64_as_string : Bool
  protojson : Bool
  emit_defaults : Bool
}

///|
pub fn protobuf_options() -> ProtobufOptions {
  {
    descriptor_bytes: 4 * 1024 * 1024,
    message_bytes: 1024 * 1024,
    depth: 64,
    items: 100000,
    fields: 10000,
    int64_as_string: false,
    protojson: false,
    emit_defaults: false,
  }
}

///|
enum ProtoValue {
  PVarint(UInt64)
  PFixed32(UInt32)
  PFixed64(UInt64)
  PBytes(Bytes)
  PStart
  PEnd
}

///|
struct ProtoItem {
  number : Int32
  value : ProtoValue
}

///|
struct ProtoWire {
  data : Bytes
  budget : Array[Int32]
  mut position : Int32
}

///|
fn proto_wire(bytes : Bytes, budget : Array[Int32]) -> ProtoWire {
  { data: bytes, budget, position: 0, }
}

///|
fn ProtoWire::take(self : ProtoWire, length : Int32) -> Bytes raise McapError {
  if length < 0 || length > self.data.length() - self.position {
    raise Invalid("truncated protobuf value")
  }
  let bytes = self.data[self.position:self.position + length].to_owned()
  self.position += length
  bytes
}

///|
fn ProtoWire::varint(self : ProtoWire) -> UInt64 raise McapError {
  let mut value = 0UL
  for i = 0; i < 10; i = i + 1 {
    let byte = self.take(1)[0]
    if i == 9 && byte > 0x01 {
      raise Invalid("protobuf varint exceeds UInt64")
    }
    value = value | ((byte & 0x7f).to_uint64() << (i * 7))
    if (byte & 0x80) == 0x00 {
      return value
    }
  }
  raise Invalid("unterminated protobuf varint")
}

///|
fn ProtoWire::next(self : ProtoWire) -> ProtoItem? raise McapError {
  if self.position == self.data.length() {
    return None
  }
  if self.budget[0] <= 0 {
    raise Invalid("protobuf item budget exceeded")
  }
  self.budget[0] -= 1
  let key = self.varint()
  if key >> 3 == 0UL || key >> 3 > 536870911UL {
    raise Invalid("invalid protobuf field number")
  }
  let value = match (key & 7UL).to_int() {
    0 => PVarint(self.varint())
    1 => PFixed64(cursor(self.take(8), default_limits()).u64())
    2 =>
      PBytes(
        self.take(
          bounded_length(
            self.varint(),
            self.data.length() - self.position,
            "protobuf value",
          ),
        ),
      )
    3 => PStart
    4 => PEnd
    5 => PFixed32(cursor(self.take(4), default_limits()).u32())
    _ => raise Invalid("invalid protobuf wire type")
  }
  Some({ number: (key >> 3).to_int(), value, })
}

///|
fn ProtoWire::skip(
  self : ProtoWire,
  item : ProtoItem,
  depth : Int32,
) -> Unit raise McapError {
  match item.value {
    PEnd => raise Invalid("unexpected protobuf end group")
    PStart => {
      if depth <= 0 {
        raise Invalid("protobuf group depth budget exceeded")
      }
      while self.next() is Some(child) {
        if child.value is PEnd {
          if child.number != item.number {
            raise Invalid("protobuf end group mismatch")
          }
          return
        }
        self.skip(child, depth - 1)
      }
      raise Invalid("unterminated protobuf group")
    }
    _ => ()
  }
}

///|
fn ProtoItem::bytes(self : ProtoItem) -> Bytes raise McapError {
  match self.value {
    PBytes(bytes) => bytes
    _ => raise Invalid("protobuf descriptor wire mismatch")
  }
}

///|
fn ProtoItem::text(self : ProtoItem) -> String raise McapError {
  @utf8.decode(self.bytes()[:]) catch {
    _ => raise Invalid("invalid protobuf UTF-8")
  }
}

///|
fn ProtoItem::number(self : ProtoItem) -> Int32 raise McapError {
  match self.value {
    PVarint(value) => value.to_uint().to_int()
    _ => raise Invalid("protobuf descriptor number mismatch")
  }
}
