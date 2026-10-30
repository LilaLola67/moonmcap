///|
pub struct Ros1Decoder {
  layout : CdrDecoder
}

///|
fn validate_ros1_time(
  name : String,
  value : Map[String, Json],
) -> Unit raise McapError {
  if name != "moonmcap_ros1/Time" && name != "moonmcap_ros1/Duration" {
    return
  }
  if value.get("nanosec") is Some(Number(number, ..)) {
    if number >= 1000000000.0 ||
      (name == "moonmcap_ros1/Time" && number < 0.0) ||
      number <= -1000000000.0 {
      raise Invalid("ROS1 time nanoseconds out of range")
    }
  }
}

///|
pub fn ros1_decoder(
  name : String,
  definition : Bytes,
  options~ : CdrOptions = cdr_options(),
) -> Ros1Decoder raise McapError {
  if options.schema_bytes < 1 ||
    options.schema_bytes > 2147482623 ||
    definition.length() > options.schema_bytes {
    raise Invalid("ROS1 schema exceeds byte budget")
  }
  let text = @utf8.decode(definition[:]) catch {
    _ => raise Invalid("invalid ros1msg UTF-8")
  }
  let lines = []
  let mut time = false
  let mut duration = false
  let mut header = false
  let header_defined = text
    .split("\n")
    .any(fn(line) { line.trim() == "MSG: std_msgs/Header" }) ||
    name == "std_msgs/Header" ||
    name == "std_msgs/msg/Header"
  for raw in text.split("\n") {
    let line = raw.split("#").next().unwrap().trim().to_string()
    if line == "" || line.has_prefix("MSG:") || line.has_prefix("====") {
      lines.push(line)
      continue
    }
    let pieces = line
      .replace_all(old="\t", new=" ")
      .split(" ")
      .filter(fn(p) { p != "" })
      .to_array()
    if pieces.length() < 2 {
      raise Invalid("ROS1 definition needs type and field name")
    }
    if pieces[1].contains("=") ||
      (pieces.length() > 2 && pieces[2].has_prefix("=")) {
      continue
    }
    if pieces.length() != 2 {
      raise Invalid("ROS1 fields cannot have defaults")
    }
    let token = pieces[0].to_string()
    if token.contains("<=") {
      raise Invalid("ROS1 bounded sequence/string syntax is unsupported")
    }
    let (base, suffix) = match token.split_once("[") {
      None => (token, "")
      Some((base, tail)) => (base.to_string(), "[" + tail.to_string())
    }
    let base = match base {
      "byte" => "int8"
      "char" => "uint8"
      "time" => {
        time = true
        "moonmcap_ros1/Time"
      }
      "duration" => {
        duration = true
        "moonmcap_ros1/Duration"
      }
      "Header" => {
        header = true
        "std_msgs/Header"
      }
      _ => base
    }
    lines.push(base + suffix + " " + pieces[1].to_string())
  }
  let separator = "\n================================================================================\nMSG: "
  let mut normalized = lines.join("\n")
  if header && !header_defined {
    normalized += separator +
      "std_msgs/Header\nuint32 seq\nmoonmcap_ros1/Time stamp\nstring frame_id"
    time = true
  }
  if time {
    normalized += separator + "moonmcap_ros1/Time\nuint32 sec\nuint32 nanosec"
  }
  if duration {
    normalized += separator + "moonmcap_ros1/Duration\nint32 sec\nint32 nanosec"
  }
  let expanded = @utf8.encode(normalized[:])
  {
    layout: cdr_decoder_internal(name, expanded, true, options={
      ..options,
      schema_bytes: options.schema_bytes + 1024,
    }),
  }
}

///|
pub fn Ros1Decoder::decode(
  self : Ros1Decoder,
  bytes : Bytes,
) -> Json raise McapError {
  if bytes.length() > self.layout.options.message_bytes {
    raise Invalid("ROS1 payload exceeds byte budget")
  }
  let wire : CdrWire = {
    bytes,
    little: true,
    aligned: false,
    position: 0,
    budget: [self.layout.options.items],
  }
  let value = self.layout.message(wire, self.layout.name, 1)
  if wire.position != bytes.length() {
    raise Invalid("trailing ROS1 payload bytes")
  }
  value
}

///|
pub fn Ros1Decoder::view(
  self : Ros1Decoder,
  bytes : Bytes,
  selection~ : DecodeSelection = decode_selection(),
) -> PayloadView raise McapError {
  self.layout.view(bytes, selection~)
}
