///|
struct ProtoField {
  name : String
  json_name : String
  number : Int
  kind : Int
  label : Int
  type_name : String
  oneof : Int?
}

///|
struct ProtoMessage {
  fields : Map[Int, ProtoField]
  map_entry : Bool
}

///|
struct ProtoEnum {
  values : Map[Int, String]
  first : Int
}

///|
pub struct ProtobufDecoder {
  messages : Map[String, ProtoMessage]
  enums : Map[String, ProtoEnum]
  options : ProtobufOptions
  field_count : Array[Int]
}

///|
fn proto_name(prefix : String, name : String) -> String {
  if prefix == "" {
    name
  } else {
    prefix + "." + name
  }
}

///|
fn proto_json_name(name : String) -> String {
  let out = StringBuilder::new()
  let mut upper = false
  for char in name {
    if char == '_' {
      upper = true
    } else {
      let code = char.to_int()
      out.write_char(
        if upper && code >= 97 && code <= 122 {
          (code - 32).unsafe_to_char()
        } else {
          char
        },
      )
      upper = false
    }
  }
  out.to_string()
}

///|
fn ProtobufDecoder::field(
  self : ProtobufDecoder,
  bytes : Bytes,
  budget : Array[Int],
) -> ProtoField raise McapError {
  let value = proto_wire(bytes, budget)
  let mut name = ""
  let mut json_name = ""
  let mut number = 0
  let mut kind = 0
  let mut label = 1
  let mut type_name = ""
  let mut oneof : Int? = None
  while value.next() is Some(item) {
    match item.number {
      1 => name = item.text()
      3 => number = item.number()
      4 => label = item.number()
      5 => kind = item.number()
      6 => type_name = item.text()
      9 => oneof = Some(item.number())
      10 => json_name = item.text()
      _ => value.skip(item, self.options.depth)
    }
  }
  if name == "" ||
    number < 1 ||
    number > 536870911 ||
    kind < 1 ||
    kind > 18 ||
    kind == 10 ||
    label < 1 ||
    label > 3 ||
    (oneof is Some(id) && (id < 0 || label == 3)) {
    raise Invalid("invalid or unsupported protobuf field descriptor")
  }
  if type_name.has_prefix(".") {
    type_name = type_name[1:].to_string()
  }
  self.field_count[0] += 1
  if self.field_count[0] > self.options.fields {
    raise Invalid("protobuf descriptor field budget exceeded")
  }
  {
    name,
    json_name: if json_name == "" {
      proto_json_name(name)
    } else {
      json_name
    },
    number,
    kind,
    label,
    type_name,
    oneof,
  }
}

///|
fn ProtobufDecoder::enum_type(
  self : ProtobufDecoder,
  prefix : String,
  bytes : Bytes,
  budget : Array[Int],
) -> Unit raise McapError {
  let value = proto_wire(bytes, budget)
  let mut name = ""
  let entries = []
  while value.next() is Some(item) {
    if item.number == 1 {
      name = item.text()
    } else if item.number == 2 {
      entries.push(item.bytes())
    } else {
      value.skip(item, self.options.depth)
    }
  }
  let full = proto_name(prefix, name)
  if name == "" || self.enums.contains(full) || self.messages.contains(full) {
    raise Invalid("duplicate or empty protobuf enum")
  }
  let values : Map[Int, String] = Map([])
  let mut first : Int? = None
  for bytes in entries {
    let entry = proto_wire(bytes, budget)
    let mut label = ""
    let mut number = 0
    while entry.next() is Some(item) {
      if item.number == 1 {
        label = item.text()
      } else if item.number == 2 {
        number = item.number()
      } else {
        entry.skip(item, self.options.depth)
      }
    }
    if label == "" {
      raise Invalid("empty protobuf enum label")
    }
    if first is None {
      first = Some(number)
    }
    if !values.contains(number) {
      values[number] = label
    }
    self.field_count[0] += 1
    if self.field_count[0] > self.options.fields {
      raise Invalid("protobuf enum value budget exceeded")
    }
  }
  if first is None {
    raise Invalid("protobuf enum has no values")
  }
  self.enums[full] = { values, first: first.unwrap(), }
}

///|
fn ProtobufDecoder::message_type(
  self : ProtobufDecoder,
  prefix : String,
  bytes : Bytes,
  budget : Array[Int],
  depth : Int,
) -> Unit raise McapError {
  if depth > self.options.depth {
    raise Invalid("protobuf descriptor depth exceeded")
  }
  let value = proto_wire(bytes, budget)
  let mut name = ""
  let fields = []
  let nested = []
  let enums = []
  let mut map_entry = false
  while value.next() is Some(item) {
    match item.number {
      1 => name = item.text()
      2 => fields.push(item.bytes())
      3 => nested.push(item.bytes())
      4 => enums.push(item.bytes())
      7 => {
        let options = proto_wire(item.bytes(), budget)
        while options.next() is Some(option) {
          if option.number == 7 {
            map_entry = option.number() != 0
          } else {
            options.skip(option, self.options.depth)
          }
        }
      }
      _ => value.skip(item, self.options.depth)
    }
  }
  let full = proto_name(prefix, name)
  if name == "" || self.messages.contains(full) || self.enums.contains(full) {
    raise Invalid("duplicate or empty protobuf message")
  }
  let by_number : Map[Int, ProtoField] = Map([])
  let names : Map[String, Bool] = Map([])
  for bytes in fields {
    let field = self.field(bytes, budget)
    if by_number.contains(field.number) || names.contains(field.json_name) {
      raise Invalid("duplicate protobuf field number or JSON name")
    }
    by_number[field.number] = field
    names[field.json_name] = true
  }
  self.messages[full] = { fields: by_number, map_entry, }
  if self.messages.length() + self.enums.length() > self.options.fields {
    raise Invalid("protobuf type budget exceeded")
  }
  for child in nested {
    self.message_type(full, child, budget, depth + 1)
  }
  for child in enums {
    self.enum_type(full, child, budget)
  }
}

///|
pub fn protobuf_decoder(
  descriptor_set : Bytes,
  options? : ProtobufOptions = protobuf_options(),
) -> ProtobufDecoder raise McapError {
  if options.descriptor_bytes < 0 ||
    descriptor_set.length() > options.descriptor_bytes ||
    options.message_bytes < 0 ||
    options.depth < 1 ||
    options.depth > 128 ||
    options.items < 1 ||
    options.fields < 1 {
    raise Invalid("invalid protobuf descriptor or budgets")
  }
  let decoder : ProtobufDecoder = {
    messages: Map([]),
    enums: Map([]),
    options,
    field_count: [0],
  }
  let budget = [options.items]
  let value = proto_wire(descriptor_set, budget)
  while value.next() is Some(item) {
    if item.number != 1 {
      value.skip(item, options.depth)
      continue
    }
    let file = proto_wire(item.bytes(), budget)
    let mut package = ""
    let messages = []
    let enums = []
    while file.next() is Some(field) {
      match field.number {
        2 => package = field.text()
        4 => messages.push(field.bytes())
        5 => enums.push(field.bytes())
        12 => {
          let syntax = field.text()
          if syntax != "" && syntax != "proto2" && syntax != "proto3" {
            raise Invalid(
              "protobuf Editions require an explicit supported decoder",
            )
          }
        }
        14 =>
          raise Invalid(
            "protobuf Editions require an explicit supported decoder",
          )
        _ => file.skip(field, options.depth)
      }
    }
    for message in messages {
      decoder.message_type(package, message, budget, 1)
    }
    for enum_type in enums {
      decoder.enum_type(package, enum_type, budget)
    }
  }
  for message in decoder.messages.values() {
    for field in message.fields.values() {
      if field.kind == 11 && !decoder.messages.contains(field.type_name) {
        raise Invalid(
          "unresolved protobuf message type; include descriptor imports",
        )
      }
      if field.kind == 14 && !decoder.enums.contains(field.type_name) {
        raise Invalid("unresolved protobuf enum type")
      }
    }
    if message.map_entry {
      if message.fields.length() != 2 ||
        !message.fields.contains(1) ||
        !message.fields.contains(2) ||
        message.fields[1].label == 3 ||
        message.fields[2].label == 3 ||
        [1, 2, 10, 11, 12, 14].contains(message.fields[1].kind) {
        raise Invalid("invalid protobuf map entry")
      }
    }
  }
  decoder
}
