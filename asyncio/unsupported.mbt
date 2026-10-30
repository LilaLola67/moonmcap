///|
pub async fn open_worker_pool(
  _path : String,
  options~ : WorkerOptions = worker_options(),
) -> WorkerPool {
  ignore(options)
  raise @mcap.io_error("codec worker pool requires Node.js")
}

///|
pub fn run_codec_worker() -> Unit raise @mcap.McapError {
  raise @mcap.io_error("codec worker host requires Node.js")
}

///|
pub async fn open_cached_http(
  _url : String,
  _directory : String,
  options~ : HttpCacheOptions = http_cache_options(),
) -> CachedHttpSource {
  ignore(options)
  raise @mcap.io_error("persistent HTTP cache requires Node.js")
}

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
  timeout_ms~ : Int32 = 5000,
  retries~ : Int32 = 2,
  max_range_bytes~ : Int32 = 32 * 1024 * 1024,
) -> AsyncSource {
  ignore(timeout_ms)
  ignore(retries)
  ignore(max_range_bytes)
  raise @mcap.io_error("HTTP adapter requires JS")
}
