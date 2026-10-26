///|
struct FieldBounds {
  mut present : Bool
  mut number_min : Json?
  mut number_max : Json?
  mut text_min : String?
  mut text_max : String?
}

///|
struct FieldZone {
  span : Int
  fields : Array[FieldBounds]
  mut unknown : Bool
}

///|
pub struct FieldIndex {
  digest : String
  size : UInt64
  decoder_identity : String
  paths : Array[String]
  zones : Map[UInt64, FieldZone]
}

///|
fn field_bounds() -> FieldBounds {
  {
    present: false,
    number_min: None,
    number_max: None,
    text_min: None,
    text_max: None,
  }
}

///|
fn FieldBounds::add(self : FieldBounds, value : Json) -> Unit raise McapError {
  self.present = true
  match value {
    Number(_, ..) => {
      ignore(decimal_parts(value.stringify()))
      if self.number_min is None ||
        (
          self.number_min is Some(old) &&
          compare_decimal(value.stringify(), old.stringify()) < 0
        ) {
        self.number_min = Some(value)
      }
      if self.number_max is None ||
        (
          self.number_max is Some(old) &&
          compare_decimal(value.stringify(), old.stringify()) > 0
        ) {
        self.number_max = Some(value)
      }
    }
    String(value) => {
      if self.text_min is None || (self.text_min is Some(old) && value < old) {
        self.text_min = Some(value)
      }
      if self.text_max is None || (self.text_max is Some(old) && value > old) {
        self.text_max = Some(value)
      }
    }
    _ => ()
  }
}

///|
fn FieldZone::charge(self : FieldZone) -> Int {
  let mut size = 128
  for field in self.fields {
    size += 128
    for value in [field.number_min, field.number_max] {
      if value is Some(value) {
        size += value.stringify().length() * 2
      }
    }
    for value in [field.text_min, field.text_max] {
      if value is Some(value) {
        size += value.length() * 2
      }
    }
  }
  size
}

///|
pub fn build_field_index(
  source : ReadSource,
  paths : Array[String],
  fingerprint : (ReadSource) -> String raise McapError,
  decoder_identity : String,
  decoder? : PayloadDecoder,
  limits? : Limits = default_limits(),
  max_zones? : Int = 10000,
  state_bytes? : Int = 8 * 1024 * 1024,
  cancelled? : () -> Bool = fn() { false },
) -> FieldIndex raise McapError {
  if paths.is_empty() ||
    paths.length() > 16 ||
    decoder_identity == "" ||
    decoder_identity.length() > 256 ||
    max_zones < 1 ||
    state_bytes < 1024 {
    raise Invalid("invalid field index settings")
  }
  let seen : Map[String, Bool] = Map([])
  let pointers = paths.map(fn(path) {
    if seen.contains(path) {
      raise Invalid("duplicate indexed field")
    }
    seen[path] = true
    json_pointer(path)
  })
  let decoder = match decoder {
    Some(decoder) => decoder
    None => standard_payload_decoder()
  }
  let declarations = empty_log()
  let zones : Map[UInt64, FieldZone] = Map([])
  let retained = [0]
  ignore(
    scan_source(
      source,
      source_visitor(
        schema=fn(schema) {
          if !declarations.schemas.any(fn(old) { old.id == schema.id }) {
            declarations.schemas.push(schema)
          }
        },
        channel=fn(channel) {
          if !declarations.channels.any(fn(old) { old.id == channel.id }) {
            declarations.channels.push(channel)
          }
        },
        message=fn(message, location) {
          let zone = match zones.get(location.offset) {
            Some(zone) => {
              if zone.span != location.span {
                raise Invalid("field index block span changed")
              }
              zone
            }
            None => {
              if zones.length() >= max_zones {
                raise Invalid("field index zone count budget exceeded")
              }
              let zone : FieldZone = {
                span: location.span,
                fields: paths.map(fn(_) { field_bounds() }),
                unknown: false,
              }
              zones[location.offset] = zone
              retained[0] += zone.charge()
              zone
            }
          }
          let prior = zone.charge()
          let channel = declarations.channels
            .iter()
            .find_first(fn(c) { c.id == message.channel_id })
          guard channel is Some(channel) else {
            raise Invalid("field index has undeclared channel")
          }
          let schema = declarations.schemas
            .iter()
            .find_first(fn(s) { s.id == channel.schema_id })
          try {
            let json = (decoder.decode_payload)(schema, channel, message.data)
            for i = 0; i < pointers.length(); i = i + 1 {
              if pointer_value(json, pointers[i]) is Some(value) {
                zone.fields[i].add(value)
              }
            }
          } catch {
            _ => zone.unknown = true
          }
          retained[0] += zone.charge() - prior
          if retained[0] > state_bytes {
            raise Invalid("field index state byte budget exceeded")
          }
        },
      ),
      limits~,
      cancelled~,
    ),
  )
  if cancelled() {
    raise Invalid("field index build cancelled")
  }
  {
    digest: checked_digest(fingerprint(source)),
    size: source.size(),
    decoder_identity,
    paths: paths.copy(),
    zones,
  }
}

///|
fn FieldBounds::json(self : FieldBounds) -> Json {
  {
    "present": self.present,
    "number_min": match self.number_min {
      None => Json::null()
      Some(v) => v
    },
    "number_max": match self.number_max {
      None => Json::null()
      Some(v) => v
    },
    "text_min": match self.text_min {
      None => Json::null()
      Some(v) => Json::string(v)
    },
    "text_max": match self.text_max {
      None => Json::null()
      Some(v) => Json::string(v)
    },
  }
}

///|
pub fn FieldIndex::to_bytes(
  self : FieldIndex,
  max_bytes? : Int = 16 * 1024 * 1024,
) -> Bytes raise McapError {
  let offsets = self.zones.keys().to_array()
  offsets.sort()
  let rows : Array[Json] = offsets.map(fn(offset) {
    let zone = self.zones[offset]
    {
      "offset": Json::string(offset.to_string()),
      "span": zone.span,
      "unknown": zone.unknown,
      "fields": Json::array(zone.fields.map(fn(f) { f.json() })),
    }
  })
  let value : Json = {
    "version": 1,
    "source_size": self.size.to_string(),
    "source_sha256": self.digest,
    "decoder_identity": self.decoder_identity,
    "paths": self.paths,
    "zones": Json::array(rows),
  }
  let bytes = @utf8.encode(value.stringify()[:])
  if max_bytes < 16 || bytes.length() > max_bytes - 16 {
    raise Invalid("field index encoded byte budget exceeded")
  }
  // Enforce the same structural/node budgets before publishing an index.
  ignore(parse_json_document(bytes, max_bytes, 32))
  b"MFLD1\r\n\x00" +
  bytes.length().to_uint().to_le_bytes() +
  bytes +
  crc32(bytes).to_le_bytes()
}

///|
fn field_object(value : Json) -> Map[String, Json] raise McapError {
  match value {
    Object(fields) => fields
    _ => raise Invalid("invalid field index object")
  }
}

///|
fn field_text(value : Json) -> String raise McapError {
  match value {
    String(text) => text
    _ => raise Invalid("invalid field index string")
  }
}

///|
fn field_bool(value : Json) -> Bool raise McapError {
  match value {
    True => true
    False => false
    _ => raise Invalid("invalid field index boolean")
  }
}

///|
fn field_array(value : Json) -> Array[Json] raise McapError {
  match value {
    Array(values) => values
    _ => raise Invalid("invalid field index array")
  }
}

///|
fn field_get(object : Map[String, Json], key : String) -> Json raise McapError {
  match object.get(key) {
    Some(v) => v
    None => raise Invalid("missing field index member: \{key}")
  }
}

///|
pub fn open_field_index(
  source : ReadSource,
  bytes : Bytes,
  fingerprint : (ReadSource) -> String raise McapError,
  decoder_identity : String,
  max_bytes? : Int = 16 * 1024 * 1024,
  max_zones? : Int = 10000,
) -> FieldIndex raise McapError {
  if bytes.length() < 16 ||
    bytes.length() > max_bytes ||
    max_zones < 1 ||
    bytes[0:8].to_owned() != b"MFLD1\r\n\x00" {
    raise Invalid("invalid field index envelope")
  }
  let header = cursor(bytes[8:12].to_owned(), default_limits())
  let length = bounded_length(
    header.u32().to_uint64(),
    max_bytes - 16,
    "field index JSON",
  )
  if length != bytes.length() - 16 {
    raise Invalid("field index length mismatch")
  }
  let data = bytes[12:12 + length].to_owned()
  if crc32(data) !=
    cursor(bytes[12 + length:].to_owned(), default_limits()).u32() {
    raise Invalid("field index CRC mismatch")
  }
  let object = field_object(parse_json_document(data, max_bytes, 32))
  if field_get(object, "version") != Json::number(1) {
    raise Invalid("unsupported field index version")
  }
  let size = parse_decimal(field_text(field_get(object, "source_size")))
  let digest = checked_digest(field_text(field_get(object, "source_sha256")))
  let identity = field_text(field_get(object, "decoder_identity"))
  if size != source.size() ||
    identity == "" ||
    identity != decoder_identity ||
    checked_digest(fingerprint(source)) != digest {
    raise Invalid("field index source or decoder identity changed")
  }
  let paths = field_array(field_get(object, "paths")).map(fn(v) {
    field_text(v)
  })
  if paths.is_empty() || paths.length() > 16 {
    raise Invalid("invalid field index path count")
  }
  let seen : Map[String, Bool] = Map([])
  for path in paths {
    ignore(json_pointer(path))
    if seen.contains(path) {
      raise Invalid("duplicate field index path")
    }
    seen[path] = true
  }
  let rows = field_array(field_get(object, "zones"))
  if rows.length() > max_zones {
    raise Invalid("field index zone count budget exceeded")
  }
  let zones : Map[UInt64, FieldZone] = Map([])
  let mut previous = 0UL
  for value in rows {
    let row = field_object(value)
    let offset = parse_decimal(field_text(field_get(row, "offset")))
    let span = bounded_length(
      parse_decimal(field_get(row, "span").stringify()),
      2147483638,
      "field index source span",
    )
    if span < 9 ||
      offset < 8UL ||
      offset < previous ||
      offset > size ||
      span.to_uint64() > size - offset {
      raise Invalid("field index zones overlap or exceed source")
    }
    previous = offset + span.to_uint64()
    let values = field_array(field_get(row, "fields"))
    if values.length() != paths.length() {
      raise Invalid("field index summary count mismatch")
    }
    let fields = values.map(fn(value) {
      let value = field_object(value)
      fn number(key : String) -> Json? raise McapError {
        let v = field_get(value, key)
        match v {
          Null => None
          Number(_, ..) => {
            ignore(decimal_parts(v.stringify()))
            Some(v)
          }
          _ => raise Invalid("invalid field index numeric bound")
        }
      }
      fn text(key : String) -> String? raise McapError {
        match field_get(value, key) {
          Null => None
          String(v) => Some(v)
          _ => raise Invalid("invalid field index text bound")
        }
      }
      let field : FieldBounds = {
        present: field_bool(field_get(value, "present")),
        number_min: number("number_min"),
        number_max: number("number_max"),
        text_min: text("text_min"),
        text_max: text("text_max"),
      }
      if (field.number_min is None) != (field.number_max is None) ||
        (field.text_min is None) != (field.text_max is None) ||
        (
          field.number_min is Some(a) &&
          field.number_max is Some(b) &&
          compare_decimal(a.stringify(), b.stringify()) > 0
        ) ||
        (field.text_min is Some(a) && field.text_max is Some(b) && a > b) ||
        (
          !field.present &&
          (field.number_min is Some(_) || field.text_min is Some(_))
        ) {
        raise Invalid("inconsistent field index bounds")
      }
      field
    })
    zones[offset] = {
      span,
      fields,
      unknown: field_bool(field_get(row, "unknown")),
    }
  }
  { digest, size, decoder_identity: identity, paths, zones, }
}
