///|
pub(all) struct HttpCacheOptions {
  page_bytes : Int32
  cache_bytes : Int32
  concurrency : Int32
  prefetch_pages : Int32
  timeout_ms : Int32
  retries : Int32
}

///|
pub fn http_cache_options() -> HttpCacheOptions {
  {
    page_bytes: 65536,
    cache_bytes: 32 * 1024 * 1024,
    concurrency: 4,
    prefetch_pages: 3,
    timeout_ms: 5000,
    retries: 2,
  }
}

///|
pub struct CachedHttpSource {
  source : AsyncSource
  snapshot : () -> String
  shutdown : async () -> Unit
}

///|
pub fn CachedHttpSource::source(self : CachedHttpSource) -> AsyncSource {
  self.source
}

///|
pub fn CachedHttpSource::metrics(
  self : CachedHttpSource,
) -> Json raise @mcap.McapError {
  @json.parse((self.snapshot)()) catch {
    _ => raise @mcap.io_error("invalid cache metrics")
  }
}

///|
/// Close settles all requests and releases the directory lease. Always await it.
pub async fn CachedHttpSource::close(self : CachedHttpSource) -> Unit {
  self.source.cancel()
  (self.shutdown)()
}
