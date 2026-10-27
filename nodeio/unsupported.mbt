///|
pub fn open_recording_directory(
  _path : String,
  resume? : Bool = false,
) -> RecordingDirectory raise @mcap.McapError {
  ignore(resume)
  raise @mcap.io_error("recording directory requires JS")
}

///|
pub fn open_managed_file(
  _path : String,
  read_ahead_bytes? : Int = 65536,
) -> ManagedFile raise @mcap.McapError {
  ignore(read_ahead_bytes)
  raise @mcap.io_error("managed files require JS")
}

///|
pub fn open_merge_job(
  _path : String,
  resume? : Bool = false,
) -> MergeJob raise @mcap.McapError {
  ignore(resume)
  raise @mcap.io_error("merge jobs require JS")
}

///|
pub fn MergeJob::checkpoint(_self : MergeJob) -> Bytes? raise @mcap.McapError {
  raise @mcap.io_error("merge jobs require JS")
}

///|
pub fn MergeJob::save(
  _self : MergeJob,
  _bytes : Bytes,
) -> Unit raise @mcap.McapError {
  raise @mcap.io_error("merge jobs require JS")
}

///|
pub fn MergeJob::storage(_self : MergeJob) -> @mcap.SortStorage {
  @mcap.sort_storage(
    fn(_) { raise @mcap.io_error("merge jobs require JS") },
    fn(_) { raise @mcap.io_error("merge jobs require JS") },
    fn(_) { () },
  )
}

///|
pub fn MergeJob::output(
  _self : MergeJob,
  _target : String,
) -> OutputTransaction raise @mcap.McapError {
  raise @mcap.io_error("merge jobs require JS")
}

///|
pub fn MergeJob::complete(
  _self : MergeJob,
  _report : Json,
) -> Unit raise @mcap.McapError {
  raise @mcap.io_error("merge jobs require JS")
}

///|
pub fn MergeJob::inspect(_self : MergeJob) -> String raise @mcap.McapError {
  raise @mcap.io_error("merge jobs require JS")
}

///|
pub fn MergeJob::close(self : MergeJob) -> Unit raise @mcap.McapError {
  self.closed = true
}

///|
pub fn MergeJob::clean(self : MergeJob) -> Unit raise @mcap.McapError {
  self.closed = true
  raise @mcap.io_error("merge jobs require JS")
}

///|
pub fn source_sha256(
  _source : @mcap.ReadSource,
) -> String raise @mcap.McapError {
  raise @mcap.io_error("SHA-256 host adapter requires JS")
}

///|
pub fn file_identity_token(_path : String) -> String raise @mcap.McapError {
  raise @mcap.io_error("file identity adapter requires JS")
}

///|
pub fn open_file(_path : String) -> @mcap.ReadSource raise @mcap.McapError {
  raise @mcap.io_error("nodeio requires the JS target and Node.js 22.3+")
}

///|
pub fn sort_directory(
  _parent : String,
) -> @mcap.SortStorage raise @mcap.McapError {
  raise @mcap.io_error("disk sort storage requires JS")
}

///|
pub fn output_transaction(
  _target : String,
) -> OutputTransaction raise @mcap.McapError {
  raise @mcap.io_error("output staging requires JS")
}

///|
pub fn OutputTransaction::publish(
  _self : OutputTransaction,
) -> Unit raise @mcap.McapError {
  raise @mcap.io_error("output staging requires JS")
}

///|
pub fn OutputTransaction::abort(
  self : OutputTransaction,
) -> Unit raise @mcap.McapError {
  self.closed = true
  raise @mcap.io_error("output staging requires JS")
}

///|
pub fn create_sink(_path : String) -> @mcap.WriteSink raise @mcap.McapError {
  raise @mcap.io_error("nodeio requires the JS target")
}

///|
pub fn write_new_file(
  _path : String,
  _bytes : Bytes,
) -> Unit raise @mcap.McapError {
  raise @mcap.io_error("nodeio requires the JS target")
}

///|
pub fn write_atomic_new(
  _path : String,
  _bytes : Bytes,
) -> Unit raise @mcap.McapError {
  raise @mcap.io_error("nodeio requires the JS target")
}

///|
pub fn read_file(
  _path : String,
  maximum? : Int = 64 * 1024 * 1024,
) -> Bytes raise @mcap.McapError {
  ignore(maximum)
  raise @mcap.io_error("nodeio requires the JS target")
}
