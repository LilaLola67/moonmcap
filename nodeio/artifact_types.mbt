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
    raise @mcap.io_error(result[1:].to_string())
  }
  result
}

///|
pub fn ArtifactDirectory::storage(
  self : ArtifactDirectory,
) -> @mcap.ArtifactBatchStorage {
  @mcap.artifact_batch_storage(
    fn() {
      let hex = self.operation("load")
      if hex == "" {
        None
      } else {
        Some(@mcap.decode_hex(hex, max_bytes=8 * 1024 * 1024))
      }
    },
    fn(bytes) { ignore(self.operation("save", bytes~)) },
    fn(id) {
      ignore(self.operation("create", id~))
      @mcap.new_write_sink(fn(bytes) {
        ignore(self.operation("write", id~, bytes~))
      })
    },
    fn(id) { ignore(self.operation("seal", id~)) },
    fn(id) { source_sha256(open_file(self.operation("source", id~))) },
    fn(id) { ignore(self.operation("publish", id~)) },
    fn(id) { self.operation("published", id~) == "true" },
    fn(id) { ignore(self.operation("discard", id~)) },
    fn() {
      let json = @json.parse(self.operation("list")[:]) catch {
        _ => raise @mcap.io_error("invalid artifact inventory")
      }
      guard json is Array(items) else {
        raise @mcap.io_error("invalid artifact inventory")
      }
      items.map(fn(item) { @mcap.parse_decimal(item.stringify()).to_int() })
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
