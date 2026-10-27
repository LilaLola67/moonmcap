///|
fn protobuf_float(value : Double) -> Json {
  if value.is_nan() {
    Json::string("NaN")
  } else if value.is_inf() {
    Json::string(if value < 0 { "-Infinity" } else { "Infinity" })
  } else {
    Json::number(value)
  }
}

///|
fn ProtobufDecoder::integer64(
  self : ProtobufDecoder,
  text : String,
  number : Double,
) -> Json {
  if self.options.int64_as_string || self.options.protojson {
    Json::string(text)
  } else {
    Json::number(number, repr=text)
  }
}

///|
fn ProtobufDecoder::scalar(
  self : ProtobufDecoder,
  field : ProtoField,
  value : ProtoValue,
) -> Json raise McapError {
  match (field.kind, value) {
    (1, PFixed64(value)) => protobuf_float(value.reinterpret_as_double())
    (2, PFixed32(value)) => {
      let number = Float::reinterpret_from_uint(value)
      let converted = number.to_double()
      if converted.is_inf() || converted.is_nan() {
        protobuf_float(converted)
      } else {
        Json::number(converted, repr=number.to_string())
      }
    }
    (3, PVarint(value)) | (16, PFixed64(value)) =>
      self.integer64(value.to_int64().to_string(), value.to_int64().to_double())
    (4, PVarint(value)) | (6, PFixed64(value)) =>
      self.integer64(value.to_string(), value.to_double())
    (5, PVarint(value)) => Json::number(value.to_uint().to_int().to_double())
    (7, PFixed32(value)) => Json::number(value.to_double())
    (8, PVarint(value)) => Json::boolean(value != 0UL)
    (9, PBytes(bytes)) =>
      Json::string(
        @utf8.decode(bytes[:]) catch {
          _ => raise Invalid("invalid protobuf string UTF-8")
        },
      )
    (12, PBytes(bytes)) => Json::string(@base64.encode(bytes[:]))
    (13, PVarint(value)) => Json::number(value.to_uint().to_double())
    (14, PVarint(_)) if self.options.protojson &&
      field.type_name == "google.protobuf.NullValue" => Json::null()
    (14, PVarint(value)) =>
      match self.enums[field.type_name].values.get(value.to_uint().to_int()) {
        Some(name) => Json::string(name)
        None => Json::number(value.to_uint().to_int().to_double())
      }
    (15, PFixed32(value)) => Json::number(value.to_int().to_double())
    (17, PVarint(value)) => {
      let number = value.to_uint()
      Json::number(((number >> 1) ^ (0U - (number & 1U))).to_int().to_double())
    }
    (18, PVarint(value)) => {
      let decoded = ((value >> 1) ^ (0UL - (value & 1UL))).to_int64()
      self.integer64(decoded.to_string(), decoded.to_double())
    }
    _ => raise Invalid("protobuf field/wire type mismatch")
  }
}

///|
fn ProtobufDecoder::default_value(
  self : ProtobufDecoder,
  field : ProtoField,
) -> Json raise McapError {
  match field.kind {
    9 | 12 => Json::string("")
    3 | 4 | 6 | 16 | 18 => self.integer64("0", 0)
    8 => Json::boolean(false)
    11 => Json::object(Map([]))
    14 if self.options.protojson &&
      field.type_name == "google.protobuf.NullValue" => Json::null()
    14 =>
      Json::string(
        self.enums[field.type_name].values[self.enums[field.type_name].first],
      )
    _ => Json::number(0)
  }
}

///|
fn protobuf_map_key(value : Json) -> String raise McapError {
  match value {
    String(value) => value
    Number(_, ..) => value.stringify()
    True => "true"
    False => "false"
    _ => raise Invalid("invalid protobuf map key")
  }
}

///|
fn protobuf_packed(kind : Int, wire : ProtoWire) -> ProtoValue raise McapError {
  if wire.budget[0] <= 0 {
    raise Invalid("protobuf packed item budget exceeded")
  }
  wire.budget[0] -= 1
  match kind {
    1 | 6 | 16 => PFixed64(cursor(wire.take(8), default_limits()).u64())
    2 | 7 | 15 => PFixed32(cursor(wire.take(4), default_limits()).u32())
    _ => PVarint(wire.varint())
  }
}

///|
fn ProtobufDecoder::message(
  self : ProtobufDecoder,
  name : String,
  bytes : Bytes,
  budget : Array[Int],
  depth : Int,
  keep_fields? : Array[String],
) -> Json raise McapError {
  if depth > self.options.depth {
    raise Invalid("protobuf message depth budget exceeded")
  }
  let model = match self.messages.get(name) {
    Some(model) => model
    None => raise Invalid("unknown protobuf message type: \{name}")
  }
  let wire = proto_wire(bytes, budget)
  let values : Map[Int, Array[ProtoValue]] = Map([])
  let oneofs : Map[Int, Int] = Map([])
  let present : Map[Int, Bool] = Map([])
  while wire.next() is Some(item) {
    guard model.fields.get(item.number) is Some(field) else {
      wire.skip(item, self.options.depth - depth)
      continue
    }
    present[field.number] = true
    if field.oneof is Some(id) {
      if oneofs.get(id) is Some(old) && old != field.number {
        values.remove(old)
      }
      oneofs[id] = field.number
    }
    if keep_fields is Some(names) && !names.contains(field.json_name) {
      wire.skip(item, self.options.depth - depth)
      continue
    }
    if field.label == 3 &&
      ![9, 11, 12].contains(field.kind) &&
      item.value is PBytes(data) {
      let packed = proto_wire(data, budget)
      let list = values.get_or_init(field.number, fn() { [] })
      while packed.position < data.length() {
        list.push(protobuf_packed(field.kind, packed))
      }
    } else if field.label == 3 || field.kind == 11 {
      values.get_or_init(field.number, fn() { [] }).push(item.value)
    } else {
      values[field.number] = [item.value]
    }
  }
  let result : Map[String, Json] = Map([])
  for field in model.fields.values() {
    if keep_fields is Some(names) && !names.contains(field.json_name) {
      if field.label == 2 && !present.contains(field.number) {
        raise Invalid("required protobuf field missing: \{field.name}")
      }
      continue
    }
    guard values.get(field.number) is Some(items) else {
      if field.label == 2 {
        raise Invalid("required protobuf field missing: \{field.name}")
      }
      if model.map_entry {
        result[field.json_name] = if field.kind == 11 {
          self.message(field.type_name, b"", budget, depth + 1)
        } else {
          self.default_value(field)
        }
      } else if self.options.protojson &&
        self.options.emit_defaults &&
        (
          field.label == 3 ||
          (
            model.proto3 &&
            field.kind != 11 &&
            field.oneof is None &&
            !field.optional
          )
        ) {
        result[field.json_name] = if field.label == 3 {
          if field.kind == 11 && self.messages[field.type_name].map_entry {
            Json::object(Map([]))
          } else {
            Json::array([])
          }
        } else {
          self.default_value(field)
        }
      }
      continue
    }
    let value = if field.kind == 11 {
      let target = self.messages[field.type_name]
      if field.label == 3 && target.map_entry {
        let entries : Map[String, Json] = Map([])
        for item in items {
          guard item is PBytes(data) else {
            raise Invalid("protobuf map requires byte entries")
          }
          let decoded = self.message(field.type_name, data, budget, depth + 1)
          guard decoded is Object(fields) else {
            raise Invalid("protobuf map entry did not decode to object")
          }
          entries[protobuf_map_key(fields[target.fields[1].json_name])] = fields[target.fields[2].json_name]
        }
        Json::object(entries)
      } else if field.label == 3 {
        let list = []
        for item in items {
          guard item is PBytes(data) else {
            raise Invalid("protobuf nested message wire mismatch")
          }
          list.push(self.message(field.type_name, data, budget, depth + 1))
        }
        Json::array(list)
      } else {
        let payload = @buffer.Buffer::Buffer()
        for item in items {
          guard item is PBytes(data) else {
            raise Invalid("protobuf nested message wire mismatch")
          }
          if data.length() > self.options.message_bytes - payload.length() {
            raise Invalid("protobuf merged message exceeds byte budget")
          }
          payload.write_bytes(data[:])
        }
        self.message(field.type_name, payload.to_bytes(), budget, depth + 1)
      }
    } else if field.label == 3 {
      Json::array(items.map(fn(item) { self.scalar(field, item) }))
    } else {
      self.scalar(field, items[items.length() - 1])
    }
    let empty_repeated = field.label == 3 &&
      value is Array(items) &&
      items.is_empty()
    if self.options.protojson && !self.options.emit_defaults && empty_repeated {
      continue
    }
    if !self.options.protojson ||
      self.options.emit_defaults ||
      !model.proto3 ||
      model.map_entry ||
      field.label == 3 ||
      field.kind == 11 ||
      field.oneof is Some(_) ||
      field.optional ||
      value != self.default_value(field) {
      result[field.json_name] = value
    }
  }
  if self.options.protojson {
    self.well_known(name, result, budget, depth)
  } else {
    Json::object(result)
  }
}

///|
pub fn ProtobufDecoder::decode(
  self : ProtobufDecoder,
  name : String,
  payload : Bytes,
) -> Json raise McapError {
  if payload.length() > self.options.message_bytes {
    raise Invalid("protobuf message byte budget exceeded")
  }
  let name = if name.has_prefix(".") { name[1:].to_string() } else { name }
  self.message(name, payload, [self.options.items], 1)
}

///|
/// Skips materializing unrequested top-level fields. Selected nested messages
/// retain normal merge/map/oneof semantics before the final nested projection.
pub fn ProtobufDecoder::decode_selected(
  self : ProtobufDecoder,
  name : String,
  payload : Bytes,
  selection : DecodeSelection,
) -> Json raise McapError {
  let paths = selection_paths(selection)
  if payload.length() > self.options.message_bytes {
    raise Invalid("protobuf message byte budget exceeded")
  }
  if self.options.protojson ||
    paths.is_empty() ||
    paths.any(fn(p) { p.is_empty() }) {
    return selected_json(self.decode(name, payload), paths, [])
  }
  let names = []
  for p in paths {
    if !names.contains(p[0]) {
      names.push(p[0])
    }
  }
  let name = if name.has_prefix(".") { name[1:].to_string() } else { name }
  selected_json(
    self.message(name, payload, [selection.scan_items], 1, keep_fields=names),
    paths,
    [],
  )
}
