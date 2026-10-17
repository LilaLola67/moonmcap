///|
pub fn open_file(_path : String) -> @mcap.ReadSource raise @mcap.McapError {
  raise @mcap.io_error("nodeio requires the JS target and Node.js 22.3+")
}
