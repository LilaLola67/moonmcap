///|
// Preserve snapshot ownership at FFI boundaries while avoiding deprecated Bytes.copy.
fn copy_payload(bytes : Bytes) -> Bytes {
  Bytes::makei(bytes.length(), i => bytes[i])
}
