///|
pub fn protobuf_json_options() -> ProtobufOptions {
  { ..protobuf_options(), protojson: true, int64_as_string: true, }
}

///|
fn proto_integer(value : Json) -> Int64 raise McapError {
  let text = match value {
    String(text) => text
    Number(_, ..) => value.stringify()
    _ => raise Invalid("invalid protobuf integer")
  }
  let negative = text.has_prefix("-")
  let digits = if negative { text[1:].to_string() } else { text }
  let magnitude = parse_decimal(digits)
  if magnitude >
    (if negative { 9223372036854775808UL } else { 9223372036854775807UL }) {
    raise Invalid("protobuf integer exceeds Int64")
  }
  (if negative { 0UL - magnitude } else { magnitude }).to_int64()
}

///|
fn proto_pad(value : Int, width : Int) -> String {
  let text = value.to_string()
  let out = StringBuilder::new()
  for i = text.length(); i < width; i = i + 1 {
    out.write_char('0')
  }
  out.write_string(text)
  out.to_string()
}

///|
fn proto_fraction(nanos : Int) -> String {
  if nanos == 0 {
    return ""
  }
  let text = proto_pad(if nanos < 0 { -nanos } else { nanos }, 9)
  "." +
  text[0:if nanos % 1000000 == 0 {
    3
  } else if nanos % 1000 == 0 {
    6
  } else {
    9
  }].to_string()
}

///|
fn proto_year_days(year : Int) -> Int {
  let previous = year - 1
  365 * previous + previous / 4 - previous / 100 + previous / 400
}

///|
fn proto_timestamp(seconds : Int64, nanos : Int64) -> String raise McapError {
  if seconds < -62135596800L ||
    seconds > 253402300799L ||
    nanos < 0L ||
    nanos > 999999999L {
    raise Invalid("invalid protobuf Timestamp range")
  }
  let mut days = seconds / 86400L
  let mut within = seconds % 86400L
  if within < 0L {
    days -= 1L
    within += 86400L
  }
  let ordinal = days.to_int() + 719162
  let mut low = 1
  let mut high = 10000
  while low + 1 < high {
    let mid = (low + high) / 2
    if proto_year_days(mid) <= ordinal {
      low = mid
    } else {
      high = mid
    }
  }
  let mut remaining = ordinal - proto_year_days(low)
  let lengths = [
    31,
    if low % 4 == 0 && (low % 100 != 0 || low % 400 == 0) {
      29
    } else {
      28
    },
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ]
  let mut month = 0
  while remaining >= lengths[month] {
    remaining -= lengths[month]
    month += 1
  }
  let clock = within.to_int()
  "\{proto_pad(low,4)}-\{proto_pad(month + 1,2)}-\{proto_pad(remaining + 1,2)}T\{proto_pad(clock / 3600,2)}:\{proto_pad(clock / 60 % 60,2)}:\{proto_pad(clock % 60,2)}\{proto_fraction(nanos.to_int())}Z"
}

///|
fn proto_special(name : String) -> Bool {
  [
    "Timestamp", "Duration", "Any", "FieldMask", "Struct", "Value", "ListValue",
    "DoubleValue", "FloatValue", "Int64Value", "UInt64Value", "Int32Value", "UInt32Value",
    "BoolValue", "StringValue", "BytesValue",
  ].any(fn(short) { name == "google.protobuf." + short })
}

///|
fn ProtobufDecoder::well_known(
  self : ProtobufDecoder,
  name : String,
  fields : Map[String, Json],
  budget : Array[Int],
  depth : Int,
) -> Json raise McapError {
  let model = self.messages[name]
  fn require(
    number : Int,
    kind : Int,
    label : Int,
    json_name : String,
  ) -> Unit raise McapError {
    guard model.fields.get(number) is Some(field) &&
      field.kind == kind &&
      field.label == label &&
      field.json_name == json_name else {
      raise Invalid("invalid well-known protobuf descriptor")
    }
  }
  if proto_special(name) {
    match name {
      "google.protobuf.Timestamp" | "google.protobuf.Duration" => {
        require(1, 3, 1, "seconds")
        require(2, 5, 1, "nanos")
      }
      "google.protobuf.Any" => {
        require(1, 9, 1, "typeUrl")
        require(2, 12, 1, "value")
      }
      "google.protobuf.Struct" => require(1, 11, 3, "fields")
      "google.protobuf.ListValue" => require(1, 11, 3, "values")
      "google.protobuf.FieldMask" => require(1, 9, 3, "paths")
      "google.protobuf.Value" =>
        for
          (number, kind, json_name) in [
            (1, 14, "nullValue"),
            (2, 1, "numberValue"),
            (3, 9, "stringValue"),
            (4, 8, "boolValue"),
            (5, 11, "structValue"),
            (6, 11, "listValue"),
          ] {
          require(number, kind, 1, json_name)
        }
      _ => {
        let kind = match name {
          "google.protobuf.DoubleValue" => 1
          "google.protobuf.FloatValue" => 2
          "google.protobuf.Int64Value" => 3
          "google.protobuf.UInt64Value" => 4
          "google.protobuf.Int32Value" => 5
          "google.protobuf.UInt32Value" => 13
          "google.protobuf.BoolValue" => 8
          "google.protobuf.StringValue" => 9
          _ => 12
        }
        require(1, kind, 1, "value")
      }
    }
  }
  fn integer(key : String) -> Int64 raise McapError {
    match fields.get(key) {
      None => 0L
      Some(value) => proto_integer(value)
    }
  }
  fn text(key : String) -> String raise McapError {
    match fields.get(key) {
      None => ""
      Some(String(value)) => value
      _ => raise Invalid("invalid protobuf special string")
    }
  }
  match name {
    "google.protobuf.Timestamp" =>
      Json::string(proto_timestamp(integer("seconds"), integer("nanos")))
    "google.protobuf.Duration" => {
      let seconds = integer("seconds")
      let nanos = integer("nanos")
      if seconds < -315576000000L ||
        seconds > 315576000000L ||
        nanos < -999999999L ||
        nanos > 999999999L ||
        (seconds < 0L && nanos > 0L) ||
        (seconds > 0L && nanos < 0L) {
        raise Invalid("invalid protobuf Duration range or sign")
      }
      Json::string(
        (if seconds < 0L || nanos < 0L { "-" } else { "" }) +
        (if seconds < 0L { -seconds } else { seconds }).to_string() +
        proto_fraction(nanos.to_int()) +
        "s",
      )
    }
    "google.protobuf.DoubleValue"
    | "google.protobuf.FloatValue"
    | "google.protobuf.Int64Value"
    | "google.protobuf.UInt64Value"
    | "google.protobuf.Int32Value"
    | "google.protobuf.UInt32Value"
    | "google.protobuf.BoolValue"
    | "google.protobuf.StringValue"
    | "google.protobuf.BytesValue" => {
      let model = self.messages[name]
      guard model.fields.get(1) is Some(field) && field.json_name == "value" else {
        raise Invalid("invalid protobuf wrapper descriptor")
      }
      fields.get_or_default("value", self.default_value(field))
    }
    "google.protobuf.Struct" =>
      fields.get_or_default("fields", Json::object(Map([])))
    "google.protobuf.ListValue" =>
      fields.get_or_default("values", Json::array([]))
    "google.protobuf.Value" => {
      if fields.length() != 1 {
        raise Invalid("protobuf Value requires one selected kind")
      }
      let (kind, value) = fields.iter().next().unwrap()
      if kind == "nullValue" {
        Json::null()
      } else if kind == "numberValue" {
        guard value is Number(_, ..) else {
          raise Invalid("protobuf Value cannot represent nonfinite numbers")
        }
        value
      } else {
        value
      }
    }
    "google.protobuf.FieldMask" => {
      let paths = match fields.get("paths") {
        None => []
        Some(Array(paths)) => paths
        _ => raise Invalid("invalid protobuf FieldMask")
      }
      let names = []
      for path in paths {
        guard path is String(path) else {
          raise Invalid("invalid protobuf field path")
        }
        let mut underscore = false
        for char in path {
          let n = char.to_int()
          if (n >= 65 && n <= 90) || (underscore && (n < 97 || n > 122)) {
            raise Invalid("FieldMask cannot convert path to camel case")
          }
          underscore = char == '_'
        }
        if underscore {
          raise Invalid("FieldMask ends in underscore")
        }
        names.push(proto_json_name(path))
      }
      Json::string(names.join(","))
    }
    "google.protobuf.Any" => {
      let url = text("typeUrl")
      let data = text("value")
      if url == "" && data == "" {
        return Json::object(Map([]))
      }
      let parts = url.split("/").to_array()
      if parts.length() < 2 || parts[parts.length() - 1] == "" {
        raise Invalid("invalid protobuf Any type URL")
      }
      let target = parts[parts.length() - 1].to_string()
      let bytes = @base64.decode(data[:]) catch {
        _ => raise Invalid("invalid protobuf Any bytes")
      }
      let value = self.message(target, bytes, budget, depth + 1)
      if proto_special(target) {
        { "@type": Json::string(url), "value": value }
      } else {
        guard value is Object(object) else {
          raise Invalid("protobuf Any target must be an object")
        }
        if object.contains("@type") {
          raise Invalid("protobuf Any target collides with @type")
        }
        object["@type"] = Json::string(url)
        Json::object(object)
      }
    }
    _ => Json::object(fields)
  }
}
