///|
pub(all) struct Limits {
  max_record_bytes : Int
  max_string_bytes : Int
  max_records : Int
  max_decompressed_bytes : Int
} derive(Eq, @debug.Debug)

///|
pub fn default_limits() -> Limits {
  {
    max_record_bytes: 16 * 1024 * 1024,
    max_string_bytes: 1024 * 1024,
    max_records: 100000,
    max_decompressed_bytes: 64 * 1024 * 1024,
  }
}

///|
pub suberror McapError {
  Invalid(String)
} derive(@debug.Debug)

///|
pub(all) struct Header {
  profile : String
  library : String
} derive(Eq, @debug.Debug)

///|
pub(all) struct Schema {
  id : Int
  name : String
  encoding : String
  data : Bytes
} derive(Eq, @debug.Debug)

///|
pub(all) struct Channel {
  id : Int
  schema_id : Int
  topic : String
  message_encoding : String
  metadata : Array[(String, String)]
} derive(Eq, @debug.Debug)

///|
pub(all) struct Message {
  channel_id : Int
  sequence : UInt
  log_time : UInt64
  publish_time : UInt64
  data : Bytes
} derive(Eq, @debug.Debug)

///|
pub(all) struct Footer {
  summary_start : UInt64
  summary_offset_start : UInt64
  summary_crc : UInt
} derive(Eq, @debug.Debug)

///|
pub(all) struct Metadata {
  name : String
  entries : Array[(String, String)]
} derive(Eq, @debug.Debug)

///|
pub(all) struct Record {
  opcode : Byte
  offset : Int
  body : Bytes
} derive(Eq, @debug.Debug)

///|
pub(all) struct Log {
  header : Header
  schemas : Array[Schema]
  channels : Array[Channel]
  messages : Array[Message]
  metadata : Array[Metadata]
  attachments : Array[Attachment]
} derive(Eq, @debug.Debug)

///|
pub fn empty_log(profile? : String = "") -> Log {
  {
    header: { profile, library: "moonmcap/0.15.22", },
    schemas: [],
    channels: [],
    messages: [],
    metadata: [],
    attachments: [],
  }
}

///|
pub fn magic() -> Bytes {
  b"\x89MCAP0\r\n"
}
