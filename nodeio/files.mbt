///|
extern "js" fn identity(path : String) -> String =
  #| (path) => { try { const s = process.getBuiltinModule('node:fs').statSync(path, {bigint:true}); if (!s.isFile()) return '!not a regular file'; return `${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`; } catch (e) { return '!' + e.message; } }

///|
#external
type FileRead

///|
extern "js" fn file_read_error(result : FileRead) -> String =
  #| result=>result.error

///|
extern "js" fn file_read_bytes(result : FileRead) -> Bytes =
  #| result=>result.bytes

///|
extern "js" fn range_bytes(
  path : String,
  expected : String,
  offset : String,
  length : Int32,
) -> FileRead =
  #| (path, expected, offset, length) => {
  #|   const fs = process.getBuiltinModule('node:fs'); let fd;
  #|   try {
  #|     fd = fs.openSync(path, 'r');
  #|     const sig = () => { const s = fs.fstatSync(fd, {bigint:true}); return `${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`; };
  #|     if (sig() !== expected) throw new Error('source changed since opening');
  #|     const position = Number(BigInt(offset));
  #|     if (!Number.isSafeInteger(position+length)) throw new Error('offset exceeds Node safe integer range');
  #|     const data = Buffer.alloc(length);
  #|     const count = fs.readSync(fd, data, 0, length, position);
  #|     if (sig() !== expected) throw new Error('source changed while reading');
  #|     return {error:'',bytes:new Uint8Array(data.buffer,data.byteOffset,count)};
  #|   } catch (e) { return {error:e.message,bytes:new Uint8Array()}; } finally { if (fd !== undefined) fs.closeSync(fd); }
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
    let result = range_bytes(path, signature, offset.to_string(), length)
    let error = file_read_error(result)
    if error != "" {
      raise @mcap.io_error(error)
    }
    file_read_bytes(result)
  })
}

///|
extern "js" fn create_exclusive(path : String) -> String =
  #| (path) => { const fs=process.getBuiltinModule('node:fs'); let fd; try { fd=fs.openSync(path,'wx'); const s=fs.fstatSync(fd,{bigint:true}); return `${s.dev}|${s.ino}`; } catch(e) { return '!'+e.message; } finally { if(fd!==undefined) fs.closeSync(fd); } }

///|
extern "js" fn append_bytes(
  path : String,
  expected : String,
  position : String,
  data : Bytes,
) -> String =
  #| (path,expected,position,data) => {
  #|   const fs=process.getBuiltinModule('node:fs'); let fd;
  #|   try {
  #|     fd=fs.openSync(path,'r+'); const s=fs.fstatSync(fd,{bigint:true});
  #|     if(`${s.dev}|${s.ino}`!==expected || s.size!==BigInt(position)) throw new Error('output changed');
  #|     const start=Number(BigInt(position));
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
    let error = append_bytes(path, signature, position[0].to_string(), bytes)
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
  let error = append_bytes(path, signature, "0", bytes)
  if error != "" {
    raise @mcap.io_error(error)
  }
}

///|
pub fn read_file(
  path : String,
  maximum~ : Int32 = 64 * 1024 * 1024,
) -> Bytes raise @mcap.McapError {
  let source = open_file(path)
  if maximum < 0 || source.size() > maximum.to_uint64() {
    raise @mcap.io_error("file exceeds memory budget")
  }
  source.read_exact(0UL, source.size().to_int())
}

///|
extern "js" fn atomic_bytes(path : String, data : Bytes) -> String =
  #| (destination,data) => {
  #|   const fs=process.getBuiltinModule('node:fs'), path=process.getBuiltinModule('node:path');
  #|   const temp=path.join(path.dirname(destination),'.'+path.basename(destination)+'.'+process.getBuiltinModule('node:crypto').randomUUID()+'.tmp');
  #|   let fd,created=false;
  #|   try {
  #|     fd=fs.openSync(temp,'wx'); created=true;
  #|     fs.writeFileSync(fd,data); fs.fsyncSync(fd); fs.closeSync(fd); fd=undefined;
  #|     fs.linkSync(temp,destination); return '';
  #|   } catch(e) { return '!'+e.message; }
  #|   finally { if(fd!==undefined) fs.closeSync(fd); if(created) fs.unlinkSync(temp); }
  #| }

///|
/// Publish a complete new file using a same-directory hard link; never replace paths.
pub fn write_atomic_new(
  path : String,
  bytes : Bytes,
) -> Unit raise @mcap.McapError {
  let error = atomic_bytes(path, bytes)
  if error != "" {
    raise @mcap.io_error(error)
  }
}
