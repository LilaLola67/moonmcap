///|
pub(all) enum NormalizedConversion {
  KeepValue
  SignedValue(Int32)
  UnsignedValue(Int32)
  ScaleValue(Double, Double)
  TextValue
  BooleanValue
}

///|
pub(all) struct NormalizedField {
  source : String
  target : String
  default_value : Json?
  required : Bool
  conversion : NormalizedConversion
}

///|
pub(all) struct NormalizedPayload {
  value : Json
  defaulted : Array[String]
  missing : Array[String]
}

///|
pub struct PayloadNormalizer {
  fields : Array[NormalizedField]
  paths : Array[(Array[String], Array[String])]
}

///|
pub fn payload_normalizer(
  fields : Array[NormalizedField],
) -> PayloadNormalizer raise McapError {
  let fields = fields.copy()
  { fields, paths: normalized_paths(fields), }
}

///|
pub fn PayloadNormalizer::normalize(
  self : PayloadNormalizer,
  document : Json,
) -> NormalizedPayload raise McapError {
  normalize_prepared(document, self.fields, self.paths)
}

///|
pub fn normalized_field(source : String, target : String) -> NormalizedField {
  {
    source,
    target,
    default_value: None,
    required: true,
    conversion: KeepValue,
  }
}

///|
fn normalize_conversion(
  value : Json,
  conversion : NormalizedConversion,
) -> Json raise McapError {
  match conversion {
    KeepValue => value
    SignedValue(bits) => {
      if ![8, 16, 32, 64].contains(bits) {
        raise Invalid("invalid normalized integer width")
      }
      let n = payload_signed(value, bits).to_int64()
      Json::number(n.to_double(), repr=n.to_string())
    }
    UnsignedValue(bits) => {
      if ![8, 16, 32, 64].contains(bits) {
        raise Invalid("invalid normalized integer width")
      }
      let n = payload_unsigned(value, bits)
      Json::number(n.to_double(), repr=n.to_string())
    }
    TextValue =>
      match value {
        String(_) => value
        _ => raise Invalid("normalized text field requires a string")
      }
    BooleanValue =>
      match value {
        True | False => value
        _ => raise Invalid("normalized boolean field requires a boolean")
      }
    ScaleValue(scale, offset) => {
      if scale.is_inf() || scale.is_nan() || offset.is_inf() || offset.is_nan() {
        raise Invalid("nonfinite normalization scale or offset")
      }
      guard value is Number(number, ..) && !number.is_inf() && !number.is_nan() else {
        raise Invalid("unit conversion requires a finite JSON number")
      }
      if compare_decimal(value.stringify(), number.to_string()) != 0 {
        raise Invalid("unit conversion would round the input number")
      }
      let result = number * scale + offset
      if result.is_nan() || result.is_inf() {
        raise Invalid("unit conversion overflow")
      }
      Json::number(result)
    }
  }
}

///|
fn normalized_paths(
  fields : Array[NormalizedField],
) -> Array[(Array[String], Array[String])] raise McapError {
  if fields.length() > 128 {
    raise Invalid("normalization field count exceeds 128")
  }
  let result : Array[(Array[String], Array[String])] = []
  let targets : Array[PayloadEdit] = []
  for field in fields {
    let source = json_pointer(field.source)
    let target = json_pointer(field.target)
    if target.is_empty() {
      raise Invalid("normalization targets must be object members")
    }
    // New targets construct objects; array indices are not inferred from names.
    result.push((source, target))
    targets.push({ path: field.target, replacement: None, })
    match field.conversion {
      SignedValue(bits) | UnsignedValue(bits) =>
        if ![8, 16, 32, 64].contains(bits) {
          raise Invalid("invalid normalized integer width")
        }
      ScaleValue(scale, offset) =>
        if scale.is_nan() ||
          scale.is_inf() ||
          offset.is_nan() ||
          offset.is_inf() {
          raise Invalid("nonfinite normalization scale or offset")
        }
      _ => ()
    }
    if field.default_value is Some(value) {
      ignore(normalize_conversion(value, field.conversion))
    }
  }
  ignore(payload_edit_paths(targets))
  result
}

///|
fn normalized_insert(
  root : Json,
  path : Array[String],
  position : Int32,
  value : Json,
) -> Json raise McapError {
  if position == path.length() {
    return value
  }
  guard root is Object(fields) else {
    raise Invalid("normalization target collision")
  }
  let copy : Map[String, Json] = Map([])
  for key in fields.keys() {
    copy[key] = fields[key]
  }
  let child = fields.get(path[position]).unwrap_or(Json::object(Map([])))
  copy[path[position]] = normalized_insert(child, path, position + 1, value)
  Json::object(copy)
}

///|
fn normalize_prepared(
  document : Json,
  fields : Array[NormalizedField],
  paths : Array[(Array[String], Array[String])],
) -> NormalizedPayload raise McapError {
  let mut result = Json::object(Map([]))
  let defaulted : Array[String] = []
  let missing : Array[String] = []
  for i = 0; i < fields.length(); i = i + 1 {
    let field = fields[i]
    let value = match pointer_value(document, paths[i].0) {
      Some(value) => value
      None =>
        match field.default_value {
          Some(value) => {
            defaulted.push(field.target)
            value
          }
          None => {
            if field.required {
              raise Invalid(
                "required normalization source absent: " + field.source,
              )
            }
            missing.push(field.target)
            continue
          }
        }
    }
    result = normalized_insert(
      result,
      paths[i].1,
      0,
      normalize_conversion(value, field.conversion),
    )
  }
  { value: result, defaulted, missing, }
}

///|
/// Explicit projection/alias/default/type/unit rules; no implicit schema repair.
pub fn normalize_payload(
  document : Json,
  fields : Array[NormalizedField],
) -> NormalizedPayload raise McapError {
  payload_normalizer(fields).normalize(document)
}

///|
/// Compose with QuerySource.decode_payload before filtering, joining or global
/// pagination. Source declarations are adapted to JSON by that existing adapter.
pub fn normalizing_decoder(
  decoder : PayloadDecoder,
  fields : Array[NormalizedField],
) -> PayloadDecoder raise McapError {
  let normalizer = payload_normalizer(fields)
  payload_decoder(fn(schema, channel, bytes) {
    normalizer.normalize((decoder.decode_payload)(schema, channel, bytes)).value
  })
}
