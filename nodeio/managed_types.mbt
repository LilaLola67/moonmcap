///|
pub(all) struct FileIoMetrics {
  physical_reads : Int
  physical_bytes : UInt64
  cached_bytes : Int
  closed : Bool
}

///|
pub struct ManagedFile {
  source : @mcap.ReadSource
  close_native : () -> Unit raise @mcap.McapError
  metrics_native : () -> FileIoMetrics
}

///|
pub fn ManagedFile::source(self : ManagedFile) -> @mcap.ReadSource {
  self.source
}

///|
pub fn ManagedFile::close(self : ManagedFile) -> Unit raise @mcap.McapError {
  (self.close_native)()
}

///|
pub fn ManagedFile::metrics(self : ManagedFile) -> FileIoMetrics {
  (self.metrics_native)()
}

///|
pub struct FileScope {
  paths : Array[String]
  files : Array[ManagedFile]
  maximum : Int
  ahead : Int
  mut closed : Bool
}

///|
pub fn file_scope(
  max_open? : Int = 64,
  read_ahead_bytes? : Int = 65536,
) -> FileScope {
  {
    paths: [],
    files: [],
    maximum: max_open,
    ahead: read_ahead_bytes,
    closed: false,
  }
}

///|
pub fn FileScope::open(
  self : FileScope,
  path : String,
) -> @mcap.ReadSource raise @mcap.McapError {
  if self.closed {
    raise @mcap.io_error("file scope is closed")
  }
  for i = 0; i < self.paths.length(); i = i + 1 {
    if self.paths[i] == path {
      return self.files[i].source
    }
  }
  if self.files.length() >= self.maximum {
    raise @mcap.io_error("open file count budget exceeded")
  }
  let file = open_managed_file(path, read_ahead_bytes=self.ahead)
  self.paths.push(path)
  self.files.push(file)
  file.source
}

///|
pub fn FileScope::close(self : FileScope) -> Unit raise @mcap.McapError {
  if self.closed {
    return
  }
  self.closed = true
  let errors = []
  for file in self.files {
    file.close() catch {
      @mcap.Invalid(reason) => errors.push(reason)
    }
  }
  self.files.clear()
  self.paths.clear()
  if !errors.is_empty() {
    raise @mcap.io_error(errors[0])
  }
}
