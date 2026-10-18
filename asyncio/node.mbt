///|
extern "js" fn file_identity(path : String) -> @js_async.Promise[String] =
  #| async path => { try { const s=await process.getBuiltinModule('node:fs/promises').stat(path,{bigint:true}); if(!s.isFile()) throw new Error('not a regular file'); return `${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`; } catch(e) { return '!'+e.message; } }

///|
#external
type FileRead

///|
extern "js" fn file_error(result : FileRead) -> String =
  #| result=>result.error

///|
extern "js" fn file_bytes(result : FileRead) -> Bytes =
  #| result=>result.bytes

///|
extern "js" fn file_range(
  path : String,
  expected : String,
  offset : String,
  length : Int,
) -> @js_async.Promise[FileRead] =
  #| async (path,expected,offset,length) => {
  #|   let file; try { file=await process.getBuiltinModule('node:fs/promises').open(path,'r');
  #|     const sig=async()=>{const s=await file.stat({bigint:true});return `${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`;};
  #|     if(await sig()!==expected) throw new Error('source changed');
  #|     const position=Number(BigInt(offset)); if(!Number.isSafeInteger(position+length)) throw new Error('unsafe file position');
  #|     const data=Buffer.alloc(length);const r=await file.read(data,0,length,position);
  #|     if(await sig()!==expected) throw new Error('source changed while reading');
  #|     return {error:'',bytes:new Uint8Array(data.buffer,data.byteOffset,r.bytesRead)};
  #|   } catch(e) {return {error:e.message,bytes:new Uint8Array()};} finally {if(file) await file.close();}
  #| }

///|
pub async fn open_file(path : String) -> AsyncSource {
  let signature = file_identity(path).wait()
  if signature.has_prefix("!") {
    raise @mcap.io_error(signature)
  }
  let size = match signature.split_once("|") {
    Some((size, _)) => @mcap.parse_decimal(size.to_string())
    None => raise @mcap.io_error("invalid async file identity")
  }
  new_source(size, async fn(offset, length) {
    let result = file_range(path, signature, offset.to_string(), length).wait()
    let error = file_error(result)
    if error != "" {
      raise @mcap.io_error(error)
    }
    file_bytes(result)
  })
}

///|
extern "js" fn create_file(path : String) -> @js_async.Promise[String] =
  #| async path => { let f; try { f=await process.getBuiltinModule('node:fs/promises').open(path,'wx'); const s=await f.stat({bigint:true}); return `${s.dev}|${s.ino}`; } catch(e) {return '!'+e.message;} finally {if(f) await f.close();} }

///|
extern "js" fn write_file(
  path : String,
  signature : String,
  offset : String,
  bytes : Bytes,
) -> @js_async.Promise[String] =
  #| async(path,signature,offset,b)=>{let f;try{f=await process.getBuiltinModule('node:fs/promises').open(path,'r+');const s=await f.stat({bigint:true});if(`${s.dev}|${s.ino}`!==signature||s.size!==BigInt(offset))throw new Error('output changed');const p=Number(BigInt(offset));if(!Number.isSafeInteger(p+b.length))throw new Error('unsafe write position');let n=0;while(n<b.length){const r=await f.write(b,n,b.length-n,p+n);if(!r.bytesWritten)throw new Error('write made no progress');n+=r.bytesWritten;}return '';}catch(e){return '!'+e.message;}finally{if(f)await f.close();}}

///|
pub async fn create_sink(path : String) -> AsyncSink {
  let signature = create_file(path).wait()
  if signature.has_prefix("!") {
    raise @mcap.io_error(signature)
  }
  let position = [0UL]
  new_sink(async fn(bytes) {
    let error = write_file(path, signature, position[0].to_string(), bytes).wait()
    if error != "" {
      raise @mcap.io_error(error)
    }
    position[0] += bytes.length().to_uint64()
  })
}
