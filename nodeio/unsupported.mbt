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
