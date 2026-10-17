///|
extern "js" fn identity(path : String) -> String =
  #| (path) => { try { const s = process.getBuiltinModule('node:fs').statSync(path, {bigint:true}); if (!s.isFile()) return '!not a regular file'; return `${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`; } catch (e) { return '!' + e.message; } }

///|
extern "js" fn range_hex(
  path : String,
  expected : String,
  offset : String,
  length : Int,
) -> String =
  #| (path, expected, offset, length) => {
  #|   const fs = process.getBuiltinModule('node:fs'); let fd;
  #|   try {
  #|     fd = fs.openSync(path, 'r');
  #|     const sig = () => { const s = fs.fstatSync(fd, {bigint:true}); return `${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`; };
  #|     if (sig() !== expected) throw new Error('source changed since opening');
  #|     const position = Number(BigInt(offset));
  #|     if (!Number.isSafeInteger(position)) throw new Error('offset exceeds Node safe integer range');
  #|     const data = Buffer.alloc(length);
  #|     const count = fs.readSync(fd, data, 0, length, position);
  #|     if (sig() !== expected) throw new Error('source changed while reading');
  #|     return data.subarray(0, count).toString('hex');
  #|   } catch (e) { return '!' + e.message; } finally { if (fd !== undefined) fs.closeSync(fd); }
  #| }

///|
pub fn open_file(path : String) -> @mcap.ReadSource raise @mcap.McapError {
  let signature = identity(path)
  if signature.has_prefix("!") {
    raise @mcap.io_error(signature)
  }
  let size = match signature.split_once("|") {
    Some((size, _)) => @mcap.parse_decimal(size.to_string())
    None => raise @mcap.io_error("invalid file identity")
  }
  @mcap.new_read_source(size, fn(offset, length) {
    let hex = range_hex(path, signature, offset.to_string(), length)
    if hex.has_prefix("!") {
      raise @mcap.io_error(hex)
    }
    @mcap.decode_hex(hex, max_bytes=length)
  })
}
