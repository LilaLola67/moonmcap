///|
pub async fn source_sha256(_source : AsyncSource) -> String {
  raise @mcap.io_error("async SHA-256 requires JS host")
}

///|
pub async fn open_file(_path : String) -> AsyncSource {
  raise @mcap.io_error("async Node file adapter requires JS")
}

///|
pub async fn create_sink(_path : String) -> AsyncSink {
  raise @mcap.io_error("async Node sink adapter requires JS")
}

///|
pub async fn open_http(
  _url : String,
  timeout_ms? : Int = 5000,
  retries? : Int = 2,
  max_range_bytes? : Int = 32 * 1024 * 1024,
) -> AsyncSource {
  ignore(timeout_ms)
  ignore(retries)
  ignore(max_range_bytes)
  raise @mcap.io_error("HTTP adapter requires JS")
}
