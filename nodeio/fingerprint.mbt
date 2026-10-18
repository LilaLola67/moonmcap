///|
#external
type DigestState

///|
extern "js" fn new_digest() -> DigestState =
  #| ()=>process.getBuiltinModule('node:crypto').createHash('sha256')

///|
extern "js" fn update_digest(state : DigestState, bytes : Bytes) -> Unit =
  #| (state,bytes)=>{state.update(bytes);}

///|
extern "js" fn finish_digest(state : DigestState) -> String =
  #| state=>state.digest('hex')

///|
pub fn source_sha256(source : @mcap.ReadSource) -> String raise @mcap.McapError {
  let state = new_digest()
  let mut offset = 0UL
  while offset < source.size() {
    let remaining = source.size() - offset
    let count = if remaining > 1048576UL { 1048576 } else { remaining.to_int() }
    update_digest(state, source.read_exact(offset, count))
    offset += count.to_uint64()
  }
  finish_digest(state)
}

///|
extern "js" fn digest_identity(text : String) -> String =
  #| text=>process.getBuiltinModule('node:crypto').createHash('sha256').update(text).digest('hex')

///|
pub fn file_identity_token(path : String) -> String raise @mcap.McapError {
  let value = identity(path)
  if value.has_prefix("!") {
    raise @mcap.io_error(value)
  }
  digest_identity(value)
}
