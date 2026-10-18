///|
pub async fn open_file(_path : String) -> AsyncSource {
  raise @mcap.io_error("async Node file adapter requires JS")
}

///|
pub async fn create_sink(_path : String) -> AsyncSink {
  raise @mcap.io_error("async Node sink adapter requires JS")
}
