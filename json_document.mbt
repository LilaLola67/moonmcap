///|
struct JsonDocumentParser {
  bytes : Bytes
  depth : Int
  mut pos : Int
  mut nodes : Int
}

///|
fn JsonDocumentParser::space(self : JsonDocumentParser) -> Unit {
  while self.pos < self.bytes.length() &&
        [b' ', b'\n', b'\r', b'\t'].contains(self.bytes[self.pos]) {
    self.pos += 1
  }
}

///|
fn JsonDocumentParser::consume(self : JsonDocumentParser, byte : Byte) -> Bool {
  self.space()
  if self.pos < self.bytes.length() && self.bytes[self.pos] == byte {
    self.pos += 1
    true
  } else {
    false
  }
}

///|
fn JsonDocumentParser::string(
  self : JsonDocumentParser,
) -> String raise McapError {
  self.space()
  let start = self.pos
  if !self.consume(b'"') {
    raise Invalid("JSON string expected")
  }
  while self.pos < self.bytes.length() {
    let byte = self.bytes[self.pos]
    self.pos += 1
    if byte == b'\\' {
      self.pos += 1
      continue
    }
    if byte == b'"' {
      let text = @utf8.decode(self.bytes[start:self.pos]) catch {
        _ => raise Invalid("invalid JSON UTF-8")
      }
      let value = @json.parse(text[:]) catch {
        _ => raise Invalid("invalid JSON string")
      }
      return match value {
        String(value) => value
        _ => raise Invalid("JSON string expected")
      }
    }
  }
  raise Invalid("unterminated JSON string")
}

///|
struct ExactDecimal {
  negative : Bool
  digits : String
  exponent : Int
}

///|
fn decimal_parts(text : String) -> ExactDecimal raise McapError {
  if text.length() == 0 || text.length() > 256 {
    raise Invalid("JSON number digit budget exceeded")
  }
  let bytes = @utf8.encode(text[:])
  let mut i = 0
  let negative = bytes[0] == b'-'
  if negative {
    i += 1
  }
  let mantissa = i
  if i >= bytes.length() {
    raise Invalid("invalid JSON number")
  }
  if bytes[i] == b'0' {
    i += 1
  } else {
    if bytes[i] < b'1' || bytes[i] > b'9' {
      raise Invalid("invalid JSON number")
    }
    while i < bytes.length() && bytes[i] >= b'0' && bytes[i] <= b'9' {
      i += 1
    }
  }
  let digits = StringBuilder::StringBuilder()
  digits.write_string(text[mantissa:i].to_string())
  let mut fraction = 0
  if i < bytes.length() && bytes[i] == b'.' {
    i += 1
    let start = i
    while i < bytes.length() && bytes[i] >= b'0' && bytes[i] <= b'9' {
      i += 1
    }
    fraction = i - start
    if fraction == 0 {
      raise Invalid("invalid JSON fraction")
    }
    digits.write_string(text[start:i].to_string())
  }
  let mut exponent = 0
  if i < bytes.length() && [b'e', b'E'].contains(bytes[i]) {
    i += 1
    let backwards = i < bytes.length() && bytes[i] == b'-'
    if i < bytes.length() && [b'-', b'+'].contains(bytes[i]) {
      i += 1
    }
    let start = i
    while i < bytes.length() && bytes[i] >= b'0' && bytes[i] <= b'9' {
      if exponent > 100000 {
        raise Invalid("JSON exponent budget exceeded")
      }
      exponent = exponent * 10 + bytes[i].to_int() - 48
      i += 1
    }
    if i == start || exponent > 1000000 {
      raise Invalid("invalid JSON exponent")
    }
    if backwards {
      exponent = -exponent
    }
  }
  if i != bytes.length() {
    raise Invalid("invalid JSON number tail")
  }
  let raw = digits.to_string()
  let mut start = 0
  let mut end = raw.length()
  while start < end && raw[start].to_int() == 48 {
    start += 1
  }
  if start == end {
    return { negative: false, digits: "0", exponent: 0, }
  }
  while end > start && raw[end - 1].to_int() == 48 {
    end -= 1
  }
  {
    negative,
    digits: raw[start:end].to_string(),
    exponent: exponent - fraction + raw.length() - end,
  }
}

///|
fn compare_decimal(a : String, b : String) -> Int raise McapError {
  let left = decimal_parts(a)
  let right = decimal_parts(b)
  if left.negative != right.negative {
    return if left.negative { -1 } else { 1 }
  }
  let sign = if left.negative { -1 } else { 1 }
  if left.digits == "0" && right.digits == "0" {
    return 0
  }
  if left.digits == "0" {
    return -sign
  }
  if right.digits == "0" {
    return sign
  }
  let order = (left.digits.length() + left.exponent).compare(
    right.digits.length() + right.exponent,
  )
  if order != 0 {
    return order * sign
  }
  for i = 0; i < left.digits.length().max(right.digits.length()); i = i + 1 {
    let a = if i < left.digits.length() { left.digits[i].to_int() } else { 48 }
    let b = if i < right.digits.length() {
      right.digits[i].to_int()
    } else {
      48
    }
    if a != b {
      return a.compare(b) * sign
    }
  }
  0
}

///|
fn JsonDocumentParser::value(
  self : JsonDocumentParser,
  depth : Int,
) -> Json raise McapError {
  self.space()
  self.nodes += 1
  if depth > self.depth ||
    self.nodes > 100000 ||
    self.pos >= self.bytes.length() {
    raise Invalid("JSON depth/node limit or truncated value")
  }
  if self.bytes[self.pos] == b'"' {
    return Json::string(self.string())
  }
  if self.consume(b'{') {
    let fields : Map[String, Json] = Map([])
    if self.consume(b'}') {
      return Json::object(fields)
    }
    while true {
      let key = self.string()
      if fields.contains(key) || !self.consume(b':') {
        raise Invalid("duplicate JSON key or missing colon")
      }
      fields[key] = self.value(depth + 1)
      if self.consume(b'}') {
        break
      }
      if !self.consume(b',') {
        raise Invalid("missing JSON object separator")
      }
    }
    return Json::object(fields)
  }
  if self.consume(b'[') {
    let values = []
    if self.consume(b']') {
      return Json::array(values)
    }
    while true {
      values.push(self.value(depth + 1))
      if self.consume(b']') {
        break
      }
      if !self.consume(b',') {
        raise Invalid("missing JSON array separator")
      }
    }
    return Json::array(values)
  }
  let start = self.pos
  while self.pos < self.bytes.length() &&
        ![b' ', b'\n', b'\r', b'\t', b',', b']', b'}'].contains(
          self.bytes[self.pos],
        ) {
    self.pos += 1
  }
  let token = @utf8.decode(self.bytes[start:self.pos]) catch {
    _ => raise Invalid("invalid JSON UTF-8")
  }
  match token {
    "null" => Json::null()
    "true" => Json::boolean(true)
    "false" => Json::boolean(false)
    _ => {
      ignore(decimal_parts(token))
      let value = @json.parse(token[:]) catch {
        _ => raise Invalid("invalid JSON number")
      }
      match value {
        Number(number, ..) => Json::number(number, repr=token)
        _ => raise Invalid("invalid JSON literal")
      }
    }
  }
}

///|
fn parse_json_document(
  bytes : Bytes,
  maximum : Int,
  depth : Int,
) -> Json raise McapError {
  if bytes.length() > maximum {
    raise Invalid("JSON payload byte budget exceeded")
  }
  let parser : JsonDocumentParser = { bytes, depth, pos: 0, nodes: 0, }
  let value = parser.value(0)
  parser.space()
  if parser.pos != bytes.length() {
    raise Invalid("JSON trailing data")
  }
  value
}
