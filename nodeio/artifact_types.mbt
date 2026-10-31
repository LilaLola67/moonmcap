///|
pub struct ArtifactDirectory {
  operate : (String, Int32, Bytes) -> String
  mut closed : Bool
}

///|
fn ArtifactDirectory::operation(
  self : ArtifactDirectory,
  op : String,
  id~ : Int32 = 0,
  bytes~ : Bytes = b"",
) -> String raise @mcap.McapError {
  if self.closed {
    raise @mcap.io_error("artifact directory is closed")
  }
  let result = (self.operate)(op, id, bytes)
  if result.has_prefix("!") {
    raise @mcap.io_error(result[1:].to_owned())
  }
  result
}

///|
pub fn ArtifactDirectory::storage(
  self : ArtifactDirectory,
) -> @mcap.ArtifactBatchStorage {
  @mcap.artifact_batch_storage(
    () => {
      let hex = self.operation("load")
      if hex == "" {
        None
      } else {
        Some(@mcap.decode_hex(hex, max_bytes=8 * 1024 * 1024))
      }
    },
    bytes => ignore(self.operation("save", bytes~)),
    id => {
      ignore(self.operation("create", id~))
      @mcap.new_write_sink(bytes => ignore(self.operation("write", id~, bytes~)))
    },
    id => ignore(self.operation("seal", id~)),
    id => source_sha256(open_file(self.operation("source", id~))),
    id => ignore(self.operation("publish", id~)),
    id => self.operation("published", id~) == "true",
    id => ignore(self.operation("discard", id~)),
    () => {
      let json = @json.parse(self.operation("list")[:]) catch {
        _ => raise @mcap.io_error("invalid artifact inventory")
      }
      guard json is Array(items) else {
        raise @mcap.io_error("invalid artifact inventory")
      }
      items.map(item => @mcap.parse_decimal(item.stringify()).to_int())
    },
  )
}

///|
pub fn ArtifactDirectory::close(
  self : ArtifactDirectory,
) -> Unit raise @mcap.McapError {
  if !self.closed {
    ignore(self.operation("unlock"))
    self.closed = true
  }
}
