///|
fn PayloadOutput::varint(
  self : PayloadOutput,
  value : UInt64,
) -> Unit raise McapError {
  let mut remaining = value
  while remaining >= 128UL {
    self.write([(remaining.to_byte() & 0x7f) | 0x80])
    remaining = remaining >> 7
  }
  self.write([remaining.to_byte()])
}

///|
fn PayloadOutput::proto_bytes(
  self : PayloadOutput,
  number : Int32,
  bytes : Bytes,
) -> Unit raise McapError {
  self.varint((number.to_uint64() << 3) | 2UL)
  self.varint(bytes.length().to_uint64())
  self.write(bytes)
}

///|
fn proto_output(
  options : ProtobufOptions,
  budget : Array[Int32],
) -> PayloadOutput {
  {
    buffer: @buffer.Buffer::Buffer(),
    maximum: options.message_bytes,
    budget,
    little: true,
    aligned: false,
  }
}

///|
fn ProtobufDecoder::encode_scalar(
  self : ProtobufDecoder,
  wire : PayloadOutput,
  field : ProtoField,
  value : Json,
  depth : Int32,
) -> Unit raise McapError {
  wire.item()
  let number = field.number.to_uint64() << 3
  match field.kind {
    1 => {
      wire.varint(number | 1UL)
      wire.unsigned(payload_float(value).reinterpret_as_uint64(), 8)
    }
    2 => {
      let n = payload_float(value)
      let f = Float::from_double(n)
      if !n.is_inf() && f.to_double().is_inf() {
        raise Invalid("protobuf float32 overflow")
      }
      wire.varint(number | 5UL)
      wire.unsigned(f.reinterpret_as_uint().to_uint64(), 4)
    }
    3 | 5 | 13 | 4 | 17 | 18 => {
      let bits = if [5, 13, 17].contains(field.kind) { 32 } else { 64 }
      let v = if field.kind == 4 || field.kind == 13 {
        payload_unsigned(value, bits)
      } else {
        payload_signed(value, bits)
      }
      let v = if field.kind == 17 || field.kind == 18 {
        (v << 1) ^
        (if v.to_int64() < 0L { 18446744073709551615UL } else { 0UL })
      } else {
        v
      }
      wire.varint(number)
      wire.varint(if field.kind == 17 { v & 4294967295UL } else { v })
    }
    6 | 7 | 15 | 16 => {
      let width = if field.kind == 7 || field.kind == 15 { 4 } else { 8 }
      let v = if field.kind == 15 || field.kind == 16 {
        payload_signed(value, width * 8)
      } else {
        payload_unsigned(value, width * 8)
      }
      wire.varint(number | (if width == 8 { 1UL } else { 5UL }))
      wire.unsigned(v, width)
    }
    8 => {
      let v = match value {
        True => 1UL
        False => 0UL
        _ => raise Invalid("protobuf boolean expected")
      }
      wire.varint(number)
      wire.varint(v)
    }
    9 => {
      guard value is String(s) else {
        raise Invalid("protobuf string expected")
      }
      wire.proto_bytes(field.number, @utf8.encode(s[:]))
    }
    12 => {
      guard value is String(s) else {
        raise Invalid("protobuf bytes require Base64 string")
      }
      let bytes = @base64.decode(s[:]) catch {
        _ => raise Invalid("invalid protobuf Base64 bytes")
      }
      wire.proto_bytes(field.number, bytes)
    }
    14 => {
      let v = match value {
        String(s) => {
          let model = self.enums[field.type_name]
          let mut found : Int32? = None
          for (n, name) in model.values {
            if name == s {
              found = Some(n)
              break
            }
          }
          match found {
            Some(n) => n.to_int64().to_uint64()
            None => raise Invalid("unknown protobuf enum name")
          }
        }
        _ => payload_signed(value, 32)
      }
      wire.varint(number)
      wire.varint(v)
    }
    11 => {
      let child = proto_output(self.options, wire.budget)
      self.encode_message(child, field.type_name, value, depth + 1)
      wire.proto_bytes(field.number, child.buffer.to_bytes())
    }
    _ => raise Invalid("unsupported protobuf encoder field kind")
  }
}

///|
fn ProtobufDecoder::encode_field(
  self : ProtobufDecoder,
  wire : PayloadOutput,
  field : ProtoField,
  value : Json,
  depth : Int32,
) -> Unit raise McapError {
  if field.kind == 11 &&
    field.label == 3 &&
    self.messages[field.type_name].map_entry {
    guard value is Object(entries) else {
      raise Invalid("protobuf map object expected")
    }
    let target = self.messages[field.type_name]
    let key_field = target.fields[1]
    let value_field = target.fields[2]
    for (key, value) in entries {
      let typed_key = match key_field.kind {
        9 => Json::string(key)
        8 =>
          match key {
            "true" => Json::boolean(true)
            "false" => Json::boolean(false)
            _ => raise Invalid("invalid protobuf map boolean key")
          }
        _ => Json::string(key)
      }
      let fields : Map[String, Json] = Map([])
      fields[key_field.json_name] = typed_key
      fields[value_field.json_name] = value
      self.encode_scalar(wire, field, Json::object(fields), depth)
    }
  } else if field.label == 3 {
    guard value is Array(items) else {
      raise Invalid("protobuf repeated array expected")
    }
    if items.length() > wire.budget[0] {
      raise Invalid("protobuf repeated item budget exceeded")
    }
    for item in items {
      self.encode_scalar(wire, field, item, depth)
    }
  } else {
    self.encode_scalar(wire, field, value, depth)
  }
}

///|
fn ProtobufDecoder::encode_message(
  self : ProtobufDecoder,
  wire : PayloadOutput,
  name : String,
  value : Json,
  depth : Int32,
) -> Unit raise McapError {
  if depth > self.options.depth {
    raise Invalid("protobuf encoding depth exceeded")
  }
  guard self.messages.get(name) is Some(model) else {
    raise Invalid("unknown protobuf type: " + name)
  }
  guard value is Object(values) else {
    raise Invalid("protobuf message object expected")
  }
  let fields = model.fields.values().to_array()
  fields.sort_by(fn(a, b) { a.number - b.number })
  for key in values.keys() {
    if !fields.any(fn(f) { f.json_name == key }) {
      raise Invalid("unknown protobuf JSON field: " + key)
    }
  }
  let oneofs : Map[Int32, Bool] = Map([])
  for field in fields {
    match values.get(field.json_name) {
      None =>
        if field.label == 2 {
          raise Invalid("required protobuf field absent")
        }
      Some(value) => {
        if !model.proto3 && field.kind == 14 {
          let values = if field.label == 3 {
            match value {
              Array(items) => items
              _ => raise Invalid("protobuf repeated array expected")
            }
          } else {
            [value]
          }
          for value in values {
            if value is Number(_, ..) &&
              !self.enums[field.type_name].values.contains(
                payload_signed(value, 32).to_uint().to_int(),
              ) {
              raise Invalid("unknown numeric enum value in proto2")
            }
          }
        }
        if field.oneof is Some(id) {
          if oneofs.contains(id) {
            raise Invalid("multiple protobuf oneof values")
          }
          oneofs[id] = true
        }
        self.encode_field(wire, field, value, depth)
      }
    }
  }
}

///|
/// Encode the decoder's ordinary object representation. ProtoJSON well-known
/// type mappings are deliberately rejected; use protobuf_options() instead.
pub fn ProtobufDecoder::encode(
  self : ProtobufDecoder,
  name : String,
  value : Json,
) -> Bytes raise McapError {
  if self.options.protojson {
    raise Invalid(
      "reverse ProtoJSON mappings are unsupported; use ordinary message objects",
    )
  }
  let name = if name.has_prefix(".") { name[1:].to_string() } else { name }
  let wire = proto_output(self.options, [self.options.items])
  self.encode_message(wire, name, value, 1)
  let bytes = wire.buffer.to_bytes()
  ignore(self.decode(name, bytes))
  bytes
}

///|
struct ProtoSlice {
  number : Int32
  value : ProtoValue
  bytes : Bytes
}

///|
fn ProtobufDecoder::rewrite_fields(
  self : ProtobufDecoder,
  name : String,
  bytes : Bytes,
  edits : Array[(Array[String], Json?)],
  budget : Array[Int32],
  depth : Int32,
) -> Bytes raise McapError {
  if depth > self.options.depth {
    raise Invalid("protobuf edit depth exceeded")
  }
  let model = self.messages[name]
  let selected : Map[Int32, Array[(Array[String], Json?)]] = Map([])
  for (path, value) in edits {
    if path.is_empty() {
      raise Invalid("protobuf root edits require explicit encode")
    }
    let mut field : ProtoField? = None
    for f in model.fields.values() {
      if f.json_name == path[0] {
        field = Some(f)
        break
      }
    }
    guard field is Some(field) else {
      raise Invalid("unknown protobuf edit field: " + path[0])
    }
    if !selected.contains(field.number) {
      selected[field.number] = []
    }
    selected[field.number].push((path[1:].to_array(), value))
  }
  // Keep raw slices of every untouched field, including unknown fields/groups.
  let wire = proto_wire(bytes, budget)
  let slices : Array[ProtoSlice] = []
  while wire.position < bytes.length() {
    let start = wire.position
    let item = wire.next().unwrap()
    wire.skip(item, self.options.depth - depth)
    slices.push({
      number: item.number,
      value: item.value,
      bytes: bytes[start:wire.position].to_owned(),
    })
  }
  let replacements : Map[Int32, Bytes] = Map([])
  let selected_oneofs : Map[Int32, Int32] = Map([])
  for (number, changes) in selected {
    let field = model.fields[number]
    let output = proto_output(self.options, budget)
    if changes[0].0.is_empty() {
      match changes[0].1 {
        None => {
          if field.label == 2 {
            raise Invalid("cannot remove required protobuf field")
          }
          if !slices.any(fn(s) { s.number == number }) {
            raise Invalid("protobuf removal field absent")
          }
        }
        Some(value) => {
          self.encode_field(output, field, value, depth)
          if field.oneof is Some(id) {
            if selected_oneofs.contains(id) {
              raise Invalid("edits select conflicting protobuf oneof fields")
            }
            selected_oneofs[id] = number
          }
        }
      }
    } else {
      if field.kind != 11 || field.label == 3 {
        raise Invalid(
          "nested protobuf edits require a singular message; replace arrays/maps explicitly",
        )
      }
      let mut combined = proto_output(self.options, budget)
      let mut occurrences = 0
      for s in slices {
        if s.number == number {
          guard s.value is PBytes(data) else {
            raise Invalid("protobuf nested edit wire mismatch")
          }
          combined.write(data)
          occurrences += 1
        } else if field.oneof is Some(id) &&
          model.fields.get(s.number) is Some(other) &&
          other.oneof == Some(id) {
          // Earlier instances were discarded when another oneof member won.
          combined = proto_output(self.options, budget)
          occurrences = 0
        }
      }
      if occurrences == 0 {
        raise Invalid("protobuf nested edit parent absent")
      }
      output.proto_bytes(
        number,
        self.rewrite_fields(
          field.type_name,
          combined.buffer.to_bytes(),
          changes,
          budget,
          depth + 1,
        ),
      )
      if field.oneof is Some(id) {
        if selected_oneofs.contains(id) {
          raise Invalid("edits select conflicting protobuf oneof fields")
        }
        selected_oneofs[id] = number
      }
    }
    replacements[number] = output.buffer.to_bytes()
  }
  let output = proto_output(self.options, budget)
  for s in slices {
    if selected.contains(s.number) {
      continue
    }
    if model.fields.get(s.number) is Some(field) &&
      field.oneof is Some(id) &&
      selected_oneofs.contains(id) {
      continue
    }
    output.write(s.bytes)
  }
  let numbers = replacements.keys().to_array()
  numbers.sort()
  for n in numbers {
    output.write(replacements[n])
  }
  output.buffer.to_bytes()
}

///|
/// Preserve untouched wire slices and unknown fields. Nested singular message
/// edits merge their occurrences; replacing an entire submessage intentionally
/// replaces its unknown contents as well.
pub fn ProtobufDecoder::rewrite(
  self : ProtobufDecoder,
  name : String,
  bytes : Bytes,
  edits : Array[PayloadEdit],
) -> Bytes raise McapError {
  if self.options.protojson {
    raise Invalid("protobuf rewrite requires ordinary object mode")
  }
  let paths = payload_edit_paths(edits)
  ignore(self.decode(name, bytes))
  let name = if name.has_prefix(".") { name[1:].to_string() } else { name }
  if edits.is_empty() {
    return bytes
  }
  let result = self.rewrite_fields(
    name,
    bytes,
    edits.mapi(fn(i, e) { (paths[i], e.replacement) }),
    [self.options.items],
    1,
  )
  ignore(self.decode(name, result))
  result
}
