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

///|
extern "js" fn create_exclusive(path : String) -> String =
  #| (path) => { const fs=process.getBuiltinModule('node:fs'); let fd; try { fd=fs.openSync(path,'wx'); const s=fs.fstatSync(fd,{bigint:true}); return `${s.dev}|${s.ino}`; } catch(e) { return '!'+e.message; } finally { if(fd!==undefined) fs.closeSync(fd); } }

///|
extern "js" fn append_hex(
  path : String,
  expected : String,
  position : String,
  hex : String,
) -> String =
  #| (path,expected,position,hex) => {
  #|   const fs=process.getBuiltinModule('node:fs'); let fd;
  #|   try {
  #|     fd=fs.openSync(path,'r+'); const s=fs.fstatSync(fd,{bigint:true});
  #|     if(`${s.dev}|${s.ino}`!==expected || s.size!==BigInt(position)) throw new Error('output changed');
  #|     const data=Buffer.from(hex,'hex'); const start=Number(BigInt(position));
  #|     if(!Number.isSafeInteger(start+data.length)) throw new Error('output offset exceeds Node safe range');
  #|     let done=0; while(done<data.length) { const n=fs.writeSync(fd,data,done,data.length-done,start+done); if(!n) throw new Error('write made no progress'); done+=n; }
  #|     return '';
  #|   } catch(e) { return '!'+e.message; } finally { if(fd!==undefined) fs.closeSync(fd); }
  #| }

///|
/// Create a new path exclusively. The sink owns it; existing files are not replaced.
pub fn create_sink(path : String) -> @mcap.WriteSink raise @mcap.McapError {
  let signature = create_exclusive(path)
  if signature.has_prefix("!") {
    raise @mcap.io_error(signature)
  }
  let position = [0UL]
  @mcap.new_write_sink(fn(bytes) {
    let error = append_hex(
      path,
      signature,
      position[0].to_string(),
      @mcap.encode_hex(bytes),
    )
    if error != "" {
      raise @mcap.io_error(error)
    }
    position[0] += bytes.length().to_uint64()
  })
}

///|
pub fn write_new_file(
  path : String,
  bytes : Bytes,
) -> Unit raise @mcap.McapError {
  let signature = create_exclusive(path)
  if signature.has_prefix("!") {
    raise @mcap.io_error(signature)
  }
  let error = append_hex(path, signature, "0", @mcap.encode_hex(bytes))
  if error != "" {
    raise @mcap.io_error(error)
  }
}

///|
pub fn read_file(
  path : String,
  maximum? : Int = 64 * 1024 * 1024,
) -> Bytes raise @mcap.McapError {
  let source = open_file(path)
  if maximum < 0 || source.size() > maximum.to_uint64() {
    raise @mcap.io_error("file exceeds memory budget")
  }
  source.read_exact(0UL, source.size().to_int())
}
