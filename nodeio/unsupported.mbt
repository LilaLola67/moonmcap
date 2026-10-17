///|
pub fn open_file(_path : String) -> @mcap.ReadSource raise @mcap.McapError {
  raise @mcap.io_error("nodeio requires the JS target and Node.js 22.3+")
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
pub fn read_file(
  _path : String,
  maximum? : Int = 64 * 1024 * 1024,
) -> Bytes raise @mcap.McapError {
  ignore(maximum)
  raise @mcap.io_error("nodeio requires the JS target")
}
