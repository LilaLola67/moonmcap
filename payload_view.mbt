///|
pub(all) struct DecodeSelection {
  paths : Array[String]
  inline_array_items : Int
  scan_items : Int
} derive(Eq, @debug.Debug)

///|
pub fn decode_selection() -> DecodeSelection {
  { paths: [], inline_array_items: 1024, scan_items: 16 * 1024 * 1024, }
}

///|
pub(all) struct BinaryField {
  path : String
  offset : Int
  byte_length : Int
  elements : Int
  element_type : String
  stride : Int
  little_endian : Bool
  aligned : Bool
} derive(Eq, @debug.Debug)

///|
pub struct PayloadView {
  source : Bytes
  value : Json
  binary : Map[String, BinaryField]
  visited : Int
}

///|
pub fn PayloadView::json(self : PayloadView) -> Json {
  self.value
}

///|
pub fn PayloadView::binary_fields(self : PayloadView) -> Array[BinaryField] {
  let keys = self.binary.keys().to_array()
  keys.sort()
  keys.map(fn(key) { self.binary[key] })
}

///|
pub fn PayloadView::info(self : PayloadView) -> Json {
  {
    "wire_bytes": self.source.length(),
    "visited_items": self.visited,
    "binary_fields": self.binary.length(),
  }
}

///|
/// Offsets are relative to a field's raw payload bytes, excluding array count.
pub fn PayloadView::read_binary(
  self : PayloadView,
  path : String,
  offset : Int,
  length : Int,
  max_bytes? : Int = 65536,
) -> Bytes raise McapError {
  guard self.binary.get(path) is Some(field) else {
    raise Invalid("binary view field not found")
  }
  if offset < 0 ||
    length < 0 ||
    max_bytes < 0 ||
    length > max_bytes ||
    offset > field.byte_length ||
    length > field.byte_length - offset {
    raise Invalid("binary view read exceeds field or output budget")
  }
  self.source[field.offset + offset:field.offset + offset + length].to_owned()
}

///|
pub fn PayloadView::write_binary(
  self : PayloadView,
  path : String,
  sink : WriteSink,
  block_bytes? : Int = 65536,
  cancelled? : () -> Bool = fn() { false },
) -> UInt64 raise McapError {
  if block_bytes < 1 || block_bytes > 1024 * 1024 {
    raise Invalid("invalid binary export block size")
  }
  guard self.binary.get(path) is Some(field) else {
    raise Invalid("binary view field not found")
  }
  let mut position = 0
  while position < field.byte_length {
    if cancelled() {
      raise Invalid("binary export cancelled")
    }
    let count = block_bytes.min(field.byte_length - position)
    sink.write(self.read_binary(path, position, count, max_bytes=block_bytes))
    position += count
  }
  field.byte_length.to_uint64()
}

///|
pub fn PayloadView::read_elements(
  self : PayloadView,
  path : String,
  start : Int,
  count : Int,
  max_elements? : Int = 1024,
) -> Json raise McapError {
  guard self.binary.get(path) is Some(field) && field.stride > 0 else {
    raise Invalid("element windows require a fixed-width primitive array")
  }
  if start < 0 ||
    count < 0 ||
    max_elements < 0 ||
    max_elements > 100000 ||
    count > max_elements ||
    start > field.elements ||
    count > field.elements - start {
    raise Invalid("array window exceeds field or item budget")
  }
  let decoder = cdr_decoder("moonmcap/View", b"uint8 unused")
  let wire : CdrWire = {
    bytes: self.source,
    little: field.little_endian,
    position: field.offset + start * field.stride,
    budget: [count],
    aligned: field.aligned,
  }
  let model : CdrField = {
    name: "value",
    base: field.element_type,
    array: None,
    bound: None,
    string_bound: None,
  }
  let values = []
  for _i = 0; _i < count; _i = _i + 1 {
    values.push(decoder.value(wire, model, 1))
  }
  Json::array(values)
}

///|
fn JsonCondition::collect_paths(
  self : JsonCondition,
  result : Array[String],
) -> Unit {
  match self {
    Compare(path, _, _) => {
      let p = view_path(path)
      if !result.contains(p) {
        result.push(p)
      }
    }
    All(items) | Any(items) =>
      for item in items {
        item.collect_paths(result)
      }
    Not(item) => item.collect_paths(result)
    _ => ()
  }
}

///|
pub fn JsonFilter::required_paths(self : JsonFilter) -> Array[String] {
  let result = []
  self.condition.collect_paths(result)
  result
}

///|
pub fn JsonFilter::selected_paths(self : JsonFilter) -> Array[String] {
  self.projections.map(fn(item) { item.0 })
}

///|
fn contains_binary_view(value : Json) -> Bool {
  match value {
    Object(fields) =>
      fields.contains("$binary") || fields.values().any(contains_binary_view)
    Array(items) => items.any(contains_binary_view)
    _ => false
  }
}

///|
/// Fail explicitly when an exact condition would inspect an opaque array.
pub fn PayloadDecoder::require_filter_fields(
  self : PayloadDecoder,
  filter : JsonFilter,
) -> PayloadDecoder raise McapError {
  let paths = filter.required_paths().map(fn(p) { json_pointer(p) })
  payload_decoder(fn(schema, channel, bytes) {
    let value = (self.decode_payload)(schema, channel, bytes)
    if channel.message_encoding != "cdr" && channel.message_encoding != "ros1" {
      return value
    }
    for path in paths {
      if pointer_value(value, path) is Some(v) && contains_binary_view(v) {
        raise Invalid(
          "exact filter requires an opaque binary field; raise inline limit or use bounded array reads",
        )
      }
    }
    value
  })
}

///|
/// Preserve JSON nesting while retaining requested paths and their ancestors.
fn selected_json(
  value : Json,
  pointers : Array[Array[String]],
  path : Array[String],
) -> Json {
  if pointers.is_empty() ||
    pointers.any(fn(p) {
      p.length() <= path.length() && p == path[0:p.length()].to_array()
    }) {
    return value
  }
  match value {
    Object(fields) => {
      let out : Map[String, Json] = Map([])
      for (key, value) in fields {
        let child = path + [key]
        if selection_wants(pointers, child) {
          out[key] = selected_json(value, pointers, child)
        }
      }
      Json::object(out)
    }
    Array(items) =>
      Json::array(
        items.mapi(fn(i, value) {
          let child = path + [i.to_string()]
          if selection_wants(pointers, child) {
            selected_json(value, pointers, child)
          } else {
            Json::null()
          }
        }),
      )
    _ => value
  }
}

///|
fn selection_paths(
  selection : DecodeSelection,
) -> Array[Array[String]] raise McapError {
  if selection.paths.length() > 256 ||
    selection.inline_array_items < 0 ||
    selection.inline_array_items > 100000 ||
    selection.scan_items < 1 {
    raise Invalid("invalid decode selection limits")
  }
  selection.paths.map(fn(p) { json_pointer(p) })
}

///|
fn selection_wants(paths : Array[Array[String]], path : Array[String]) -> Bool {
  paths.is_empty() ||
  paths.any(fn(p) {
    let common = p.length().min(path.length())
    for i = 0; i < common; i = i + 1 {
      if p[i] != path[i] {
        return false
      }
    }
    true
  })
}

///|
fn view_path(parts : Array[String]) -> String {
  parts.fold(init="", fn(path, part) {
    path +
    "/" +
    part.replace_all(old="~", new="~0").replace_all(old="/", new="~1")
  })
}

///|
fn BinaryField::json(self : BinaryField) -> Json {
  {
    "$binary": self.path,
    "byte_length": self.byte_length,
    "elements": self.elements,
    "element_type": self.element_type,
    "stride": self.stride,
    "little_endian": self.little_endian,
  }
}

///|
fn cdr_width(base : String) -> Int {
  match base {
    "bool" | "byte" | "char" | "uint8" | "int8" => 1
    "uint16" | "int16" => 2
    "uint32" | "int32" | "float32" => 4
    "uint64" | "int64" | "float64" => 8
    _ => 0
  }
}

///|
fn CdrWire::skip(self : CdrWire, length : Int) -> Unit raise McapError {
  if length < 0 || length > self.bytes.length() - self.position {
    raise Invalid("truncated projected payload")
  }
  self.position += length
}

///|
struct CdrProjection {
  decoder : CdrDecoder
  paths : Array[Array[String]]
  selection : DecodeSelection
  binary : Map[String, BinaryField]
}

///|
fn CdrProjection::message(
  self : CdrProjection,
  wire : CdrWire,
  name : String,
  path : Array[String],
  depth : Int,
  emit : Bool,
) -> Json raise McapError {
  if depth > self.decoder.options.depth {
    raise Invalid("projected CDR depth exceeded")
  }
  let result : Map[String, Json] = Map([])
  let fields = self.decoder.definitions[name]
  if fields.is_empty() && !self.decoder.ros1 {
    wire.item()
    if wire.unsigned(1) != 0UL {
      raise Invalid("invalid empty ROS2 message")
    }
  }
  for field in fields {
    let child = path + [field.name]
    let wanted = emit && selection_wants(self.paths, child)
    let scalar = fn(
      emit : Bool,
      item_path : Array[String],
    ) -> Json raise McapError {
      if cdr_primitive(field.base) {
        self.decoder.value(wire, field, depth)
      } else {
        wire.item()
        self.message(wire, field.base, item_path, depth + 1, emit)
      }
    }
    let value = match field.array {
      None => scalar(wanted, child)
      Some(array) => {
        wire.item()
        let count = if array < 0 {
          bounded_length(wire.unsigned(4), wire.budget[0], "projected array")
        } else {
          array
        }
        if count > wire.budget[0] ||
          (field.bound is Some(bound) && count > bound) {
          raise Invalid("projected array item or declared limit")
        }
        let width = cdr_width(field.base)
        if wanted &&
          count > self.selection.inline_array_items &&
          self.paths.any(fn(p) {
            p.length() > child.length() &&
            p[0:child.length()].to_array() == child
          }) {
          raise Invalid(
            "array element selection exceeds inline limit; use bounded read_elements",
          )
        }
        if width > 0 && (!wanted || count > self.selection.inline_array_items) {
          if count > 0 && wire.aligned {
            wire.skip((width - (wire.position - 4) % width) % width)
          }
          let start = wire.position
          if count > (wire.bytes.length() - start) / width {
            raise Invalid("truncated projected primitive array")
          }
          if field.base == "bool" {
            for i = 0; i < count; i = i + 1 {
              if wire.bytes[start + i] > 0x01 {
                raise Invalid("invalid projected boolean")
              }
            }
          }
          wire.skip(count * width)
          wire.budget[0] -= count
          if wanted {
            let binary : BinaryField = {
              path: view_path(child),
              offset: start,
              byte_length: count * width,
              elements: count,
              element_type: field.base,
              stride: width,
              little_endian: wire.little,
              aligned: wire.aligned,
            }
            self.binary[binary.path] = binary
            binary.json()
          } else {
            Json::null()
          }
        } else {
          let start = wire.position
          let inline = wanted && count <= self.selection.inline_array_items
          let values = []
          for i = 0; i < count; i = i + 1 {
            let value = scalar(inline, child + [i.to_string()])
            if inline {
              values.push(value)
            }
          }
          if inline {
            Json::array(values)
          } else if wanted {
            let binary : BinaryField = {
              path: view_path(child),
              offset: start,
              byte_length: wire.position - start,
              elements: count,
              element_type: field.base,
              stride: 0,
              little_endian: wire.little,
              aligned: wire.aligned,
            }
            self.binary[binary.path] = binary
            binary.json()
          } else {
            Json::null()
          }
        }
      }
    }
    if self.decoder.ros1 &&
      (name == "moonmcap_ros1/Time" || name == "moonmcap_ros1/Duration") &&
      field.name == "nanosec" {
      validate_ros1_time(name, Map::from_array([("nanosec", value)]))
    }
    if wanted {
      result[field.name] = value
    }
  }
  Json::object(result)
}

///|
pub fn CdrDecoder::view(
  self : CdrDecoder,
  bytes : Bytes,
  selection? : DecodeSelection = decode_selection(),
) -> PayloadView raise McapError {
  let paths = selection_paths(selection)
  if bytes.length() > self.options.message_bytes {
    raise Invalid("projected payload exceeds byte budget")
  }
  if !self.ros1 &&
    (
      bytes.length() < 4 ||
      bytes[0] != 0x00 ||
      (bytes[1] != 0x00 && bytes[1] != 0x01) ||
      bytes[2] != 0x00 ||
      bytes[3] != 0x00
    ) {
    raise Invalid("invalid projected XCDR1 encapsulation or size")
  }
  let wire : CdrWire = {
    bytes,
    little: if self.ros1 {
      true
    } else {
      bytes[1] == 0x01
    },
    position: if self.ros1 {
      0
    } else {
      4
    },
    budget: [selection.scan_items],
    aligned: !self.ros1,
  }
  let projection : CdrProjection = {
    decoder: self,
    paths,
    selection,
    binary: Map([]),
  }
  let value = projection.message(wire, self.name, [], 1, true)
  if (self.ros1 && bytes.length() != wire.position) ||
    bytes.length() - wire.position > 3 ||
    bytes[wire.position:].iter().any(fn(b) { b != 0x00 }) {
    raise Invalid("trailing projected CDR bytes")
  }
  {
    source: bytes,
    value,
    binary: projection.binary,
    visited: selection.scan_items - wire.budget[0],
  }
}
