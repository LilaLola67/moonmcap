///|
pub(all) struct CdrOptions {
  schema_bytes : Int32
  message_bytes : Int32
  string_bytes : Int32
  items : Int32
  fields : Int32
  depth : Int32
  int64_as_string : Bool
}

///|
pub fn cdr_options() -> CdrOptions {
  {
    schema_bytes: 4 * 1024 * 1024,
    message_bytes: 16 * 1024 * 1024,
    string_bytes: 1024 * 1024,
    items: 100000,
    fields: 10000,
    depth: 64,
    int64_as_string: false,
  }
}

///|
struct CdrField {
  name : String
  base : String
  array : Int32?
  bound : Int32?
  string_bound : Int32?
}

///|
pub struct CdrDecoder {
  name : String
  definitions : Map[String, Array[CdrField]]
  options : CdrOptions
  field_count : Int32
  ros1 : Bool
}

///|
fn cdr_name(name : String, package : String) -> String raise McapError {
  let parts = name.split("/").to_array()
  let normalized = if parts.length() == 1 && package != "" {
    package + "/" + name
  } else if parts.length() == 2 {
    name
  } else if parts.length() == 3 && parts[1] == "msg" {
    parts[0].to_string() + "/" + parts[2].to_string()
  } else {
    raise Invalid("CDR type requires a ROS message resource name")
  }
  if !normalized.split("/").all(fn(part) { proto_identifier(part.to_string()) }) {
    raise Invalid("invalid CDR resource name")
  }
  normalized
}

///|
fn cdr_primitive(name : String) -> Bool {
  [
    "bool", "byte", "char", "int8", "uint8", "int16", "uint16", "int32", "uint32",
    "int64", "uint64", "float32", "float64", "string",
  ].contains(name)
}

///|
fn cdr_bound(text : String, maximum : Int32) -> Int32 raise McapError {
  let value = parse_decimal(text)
  if value > maximum.to_uint64() {
    raise Invalid("CDR declared bound exceeds configured budget")
  }
  value.to_int()
}

///|
fn cdr_field(
  token : String,
  name : String,
  package : String,
  options : CdrOptions,
  ros1 : Bool,
) -> CdrField raise McapError {
  if !proto_identifier(name) {
    raise Invalid("invalid CDR field name")
  }
  let mut base = token
  let mut array : Int32? = None
  let mut bound : Int32? = None
  let mut string_bound : Int32? = None
  if token.split_once("[") is Some((left, right)) {
    if !right.has_suffix("]") {
      raise Invalid("malformed CDR array type")
    }
    let count = right[0:right.length() - 1].to_string()
    base = left.to_string()
    if count == "" {
      array = Some(-1)
    } else if count.has_prefix("<=") {
      array = Some(-1)
      bound = Some(cdr_bound(count[2:].to_string(), options.items))
    } else {
      array = Some(cdr_bound(count, options.items))
    }
    if !ros1 && (array == Some(0) || bound == Some(0)) {
      raise Invalid("CDR array bound must be positive")
    }
  }
  if base.has_prefix("string<=") {
    string_bound = Some(cdr_bound(base[8:].to_string(), options.string_bytes))
    base = "string"
  }
  if !cdr_primitive(base) {
    if base == "wstring" || base.has_prefix("wstring<=") {
      raise Invalid("CDR wstring is outside the supported XCDR1 subset")
    }
    base = if base == "time" {
      "builtin_interfaces/Time"
    } else if base == "duration" {
      "builtin_interfaces/Duration"
    } else {
      cdr_name(base, package)
    }
  }
  { name, base, array, bound, string_bound, }
}

///|
fn cdr_check_graph(
  name : String,
  definitions : Map[String, Array[CdrField]],
  active : Map[String, Bool],
  done : Map[String, Int32],
  depth : Int32,
  maximum : Int32,
) -> Int32 raise McapError {
  if depth > maximum {
    raise Invalid("CDR schema nesting exceeds depth budget")
  }
  if active.contains(name) {
    raise Invalid("recursive CDR message definitions are unsupported")
  }
  if done.get(name) is Some(height) {
    if depth + height - 1 > maximum {
      raise Invalid("CDR schema nesting exceeds depth budget")
    }
    return height
  }
  guard definitions.get(name) is Some(fields) else {
    raise Invalid("missing CDR message dependency: \{name}")
  }
  active[name] = true
  let mut height = 1
  for field in fields {
    if !cdr_primitive(field.base) {
      height = height.max(
        1 +
        cdr_check_graph(
          field.base,
          definitions,
          active,
          done,
          depth + 1,
          maximum,
        ),
      )
    }
  }
  active.remove(name)
  done[name] = height
  height
}

///|
fn cdr_decoder_internal(
  name : String,
  definition : Bytes,
  ros1 : Bool,
  options~ : CdrOptions = cdr_options(),
) -> CdrDecoder raise McapError {
  if options.schema_bytes < 1 ||
    definition.length() > options.schema_bytes ||
    options.message_bytes < 4 ||
    options.string_bytes < 1 ||
    options.items < 1 ||
    options.fields < 1 ||
    options.depth < 1 ||
    options.depth > 128 {
    raise Invalid("invalid CDR definition or budgets")
  }
  let name = cdr_name(name, "")
  let text = @utf8.decode(definition[:]) catch {
    _ => raise Invalid("invalid ros2msg UTF-8")
  }
  let definitions : Map[String, Array[CdrField]] = Map([])
  let mut current = name
  definitions[current] = []
  let mut separator = false
  let mut count = 0
  for raw in text.split("\n") {
    let line = raw.split("#").next().unwrap().trim().to_string()
    if line == "" {
      continue
    }
    if line ==
      "================================================================================" {
      if separator {
        raise Invalid("duplicate CDR dependency separator")
      }
      separator = true
      continue
    }
    if line.has_prefix("MSG: ") {
      if !separator {
        raise Invalid("ros2msg dependency requires a separator")
      }
      current = cdr_name(line[5:].trim().to_string(), "")
      if definitions.contains(current) {
        raise Invalid("duplicate CDR type definition")
      }
      definitions[current] = []
      separator = false
      continue
    }
    if separator {
      raise Invalid("missing ros2msg dependency name")
    }
    let pieces = line
      .replace_all(old="\t", new=" ")
      .split(" ")
      .filter(fn(s) { s != "" })
      .to_array()
    if pieces.length() < 2 {
      raise Invalid("CDR definition needs type and field name")
    }
    // Constants and field defaults do not occupy wire bytes.
    if pieces[1].split_once("=") is Some(_) ||
      (pieces.length() > 2 && pieces[2].has_prefix("=")) {
      continue
    }
    let package = current.split("/").next().unwrap().to_string()
    let field = cdr_field(
      pieces[0].to_string(),
      pieces[1].to_string(),
      package,
      options,
      ros1,
    )
    if definitions[current].any(fn(existing) { existing.name == field.name }) {
      raise Invalid("duplicate CDR field")
    }
    definitions[current].push(field)
    count += 1
    if count > options.fields || definitions.length() > options.fields {
      raise Invalid("CDR schema field/type budget exceeded")
    }
  }
  if separator {
    raise Invalid("unfinished ros2msg dependency separator")
  }
  if definitions.length() > options.fields {
    raise Invalid("CDR schema type budget exceeded")
  }
  let done : Map[String, Int32] = Map([])
  for key in definitions.keys() {
    ignore(cdr_check_graph(key, definitions, Map([]), done, 1, options.depth))
  }
  { name, definitions, options, field_count: count, ros1, }
}

///|
pub fn cdr_decoder(
  name : String,
  definition : Bytes,
  options~ : CdrOptions = cdr_options(),
) -> CdrDecoder raise McapError {
  cdr_decoder_internal(name, definition, false, options~)
}
