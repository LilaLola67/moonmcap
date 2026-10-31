///|
pub struct RecordingDirectory {
  operate : (String, Int32, Bytes) -> String
  mut closed : Bool
}

///|
fn RecordingDirectory::operation(
  self : RecordingDirectory,
  op : String,
  id~ : Int32 = 0,
  bytes~ : Bytes = b"",
) -> String raise @mcap.McapError {
  if self.closed {
    raise @mcap.io_error("recording directory is closed")
  }
  let result = (self.operate)(op, id, bytes)
  if result.has_prefix("!") {
    raise @mcap.io_error(result[1:].to_owned())
  }
  result
}

///|
pub fn RecordingDirectory::manifest(
  self : RecordingDirectory,
) -> Bytes? raise @mcap.McapError {
  let hex = self.operation("load")
  if hex == "" {
    None
  } else {
    Some(@mcap.decode_hex(hex, max_bytes=8 * 1024 * 1024))
  }
}

///|
pub fn RecordingDirectory::storage(
  self : RecordingDirectory,
) -> @mcap.RecordingStorage {
  @mcap.recording_storage(
    id => {
      ignore(self.operation("create", id~))
      @mcap.new_write_sink(bytes => ignore(self.operation("write", id~, bytes~)))
    },
    id => open_file(self.operation("source", id~)),
    id => self.operation("exists", id~) == "true",
    id => ignore(self.operation("seal", id~)),
    bytes => ignore(self.operation("save", bytes~)),
    id => ignore(self.operation("quarantine", id~)),
    () => {
      let value = @json.parse(self.operation("list")[:]) catch {
        _ => raise @mcap.io_error("invalid recording file inventory")
      }
      guard value is Array(items) else {
        raise @mcap.io_error("invalid recording file inventory")
      }
      items.map(item => @mcap.parse_decimal(item.stringify()).to_int())
    },
    source_sha256,
  )
}

///|
pub fn RecordingDirectory::inspect(
  self : RecordingDirectory,
) -> Json raise @mcap.McapError {
  let files = @json.parse(self.operation("inspect")[:]) catch {
    _ => raise @mcap.io_error("invalid recording inventory")
  }
  {
    "files": files,
    "manifest": match self.manifest() {
      None => Json::null()
      Some(bytes) => @mcap.inspect_recording_manifest(bytes)
    },
  }
}

///|
pub fn RecordingDirectory::close(
  self : RecordingDirectory,
) -> Unit raise @mcap.McapError {
  if !self.closed {
    ignore(self.operation("unlock"))
    self.closed = true
  }
}
