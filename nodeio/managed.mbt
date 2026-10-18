///|
#external
type NativeFile

///|
extern "js" fn managed_open(path : String, ahead : Int) -> NativeFile =
  #| (path,ahead)=>{const fs=process.getBuiltinModule('node:fs');let fd;try{fd=fs.openSync(path,'r');const s=fs.fstatSync(fd,{bigint:true});if(!s.isFile())throw new Error('not a regular file');const signature=`${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`;return {path,fd,ahead,signature,size:String(s.size),cache:new Uint8Array(),start:0,error:'',reads:0,bytes:0n,closed:false};}catch(e){if(fd!==undefined)fs.closeSync(fd);return {error:e.message};}}

///|
extern "js" fn managed_error(state : NativeFile) -> String =
  #| s=>s.error

///|
extern "js" fn managed_size(state : NativeFile) -> String =
  #| s=>s.size

///|
extern "js" fn managed_read(
  state : NativeFile,
  offset : String,
  length : Int,
) -> FileRead =
  #| (s,offset,length)=>{const fs=process.getBuiltinModule('node:fs');try{
  #|   if(s.closed)throw new Error('managed file is closed');const sig=t=>`${t.size}|${t.dev}|${t.ino}|${t.mtimeNs}|${t.ctimeNs}`;
  #|   if(sig(fs.fstatSync(s.fd,{bigint:true}))!==s.signature||sig(fs.statSync(s.path,{bigint:true}))!==s.signature)throw new Error('managed source changed');
  #|   const position=Number(BigInt(offset));if(!Number.isSafeInteger(position+length))throw new Error('unsafe read offset');
  #|   if(position>=s.start&&position+length<=s.start+s.cache.length)return {error:'',bytes:s.cache.slice(position-s.start,position-s.start+length)};
  #|   const count=Math.min(Math.max(length,s.ahead),Number(BigInt(s.size)-BigInt(offset)));const data=new Uint8Array(count);let done=0;
  #|   while(done<count){const n=fs.readSync(s.fd,data,done,count-done,position+done);s.reads++;s.bytes+=BigInt(n);if(!n)throw new Error('managed read made no progress');done+=n;}
  #|   if(sig(fs.fstatSync(s.fd,{bigint:true}))!==s.signature)throw new Error('managed source changed during read');
  #|   if(count<=s.ahead){s.cache=data;s.start=position;return {error:'',bytes:data.slice(0,length)};}s.cache=new Uint8Array();return {error:'',bytes:data};
  #| }catch(e){return {error:e.message,bytes:new Uint8Array()};}}

///|
extern "js" fn managed_close(state : NativeFile) -> String =
  #| s=>{if(s.closed)return '';s.closed=true;s.cache=new Uint8Array();try{process.getBuiltinModule('node:fs').closeSync(s.fd);return '';}catch(e){return e.message;}}

///|
extern "js" fn managed_counter(state : NativeFile, field : Int) -> Int =
  #| (s,f)=>f===0?s.reads:f===1?s.cache.length:s.closed?1:0

///|
extern "js" fn managed_bytes(state : NativeFile) -> String =
  #| s=>String(s.bytes)

///|
pub fn open_managed_file(
  path : String,
  read_ahead_bytes? : Int = 65536,
) -> ManagedFile raise @mcap.McapError {
  if read_ahead_bytes < 0 || read_ahead_bytes > 16 * 1024 * 1024 {
    raise @mcap.io_error("read-ahead budget must be 0..16 MiB")
  }
  let state = managed_open(path, read_ahead_bytes)
  let error = managed_error(state)
  if error != "" {
    raise @mcap.io_error(error)
  }
  let size = @mcap.parse_decimal(managed_size(state))
  let source = @mcap.new_read_source(size, fn(offset, length) {
    let result = managed_read(state, offset.to_string(), length)
    let error = file_read_error(result)
    if error != "" {
      raise @mcap.io_error(error)
    }
    file_read_bytes(result)
  })
  {
    source,
    close_native: fn() {
      let error = managed_close(state)
      if error != "" {
        raise @mcap.io_error(error)
      }
    },
    metrics_native: fn() {
      {
        physical_reads: managed_counter(state, 0),
        physical_bytes: @mcap.parse_decimal(managed_bytes(state)) catch {
          _ => 0UL
        },
        cached_bytes: managed_counter(state, 1),
        closed: managed_counter(state, 2) != 0,
      }
    },
  }
}
