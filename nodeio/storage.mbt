///|
extern "js" fn create_job(parent : String) -> String =
  #| parent=>{try{return process.getBuiltinModule('node:fs').mkdtempSync(process.getBuiltinModule('node:path').join(parent,'moonmcap-sort-'));}catch(e){return '!'+e.message;}}

///|
extern "js" fn remove_owned(path : String, directory : Bool) -> String =
  #| (path,directory)=>{try{const fs=process.getBuiltinModule('node:fs');if(directory)fs.rmdirSync(path);else fs.unlinkSync(path);return '';}catch(e){return e.code==='ENOENT'?'':'!'+e.message;}}

///|
pub fn sort_directory(
  parent : String,
) -> @mcap.SortStorage raise @mcap.McapError {
  let job = create_job(parent)
  if job.has_prefix("!") {
    raise @mcap.io_error(job)
  }
  @mcap.sort_storage(
    fn(id) { create_sink("\{job}/\{id}.run") },
    fn(id) { open_file("\{job}/\{id}.run") },
    fn(id) {
      let error = remove_owned("\{job}/\{id}.run", false)
      if error != "" {
        raise @mcap.io_error(error)
      }
    },
    finish=fn() {
      let error = remove_owned(job, true)
      if error != "" {
        raise @mcap.io_error(error)
      }
    },
  )
}

///|
extern "js" fn stage_path(target : String) -> String =
  #| target=>{try{const fs=process.getBuiltinModule('node:fs'),p=process.getBuiltinModule('node:path');try{fs.lstatSync(target);return '!output already exists';}catch(e){if(e.code!=='ENOENT')throw e;}return p.join(p.dirname(target),'.'+p.basename(target)+'.'+process.getBuiltinModule('node:crypto').randomUUID()+'.part');}catch(e){return '!'+e.message;}}

///|
extern "js" fn file_key(path : String) -> String =
  #| path=>{try{const s=process.getBuiltinModule('node:fs').lstatSync(path,{bigint:true});if(!s.isFile())throw new Error('staging output is not a file');return `${s.dev}|${s.ino}`;}catch(e){return '!'+e.message;}}

///|
extern "js" fn publish_stage(
  path : String,
  target : String,
  key : String,
  size : String,
) -> String =
  #| (path,target,key,size)=>{let fd,published=false,error='';const fs=process.getBuiltinModule('node:fs');try{const s=fs.lstatSync(path,{bigint:true});if(!s.isFile()||`${s.dev}|${s.ino}`!==key||s.size!==BigInt(size))throw new Error('staging output changed');fd=fs.openSync(path,'r+');fs.fsyncSync(fd);const closing=fd;fd=undefined;fs.closeSync(closing);fs.linkSync(path,target);published=true;fs.unlinkSync(path);}catch(e){error=e.message;}finally{if(fd!==undefined)try{fs.closeSync(fd);}catch(e){error+=(error?'; ':'')+'close cleanup: '+e.message;}}return error?'!'+(published?'output published; ':'')+error:'';}

///|
pub fn output_transaction(
  target : String,
) -> OutputTransaction raise @mcap.McapError {
  let temporary = stage_path(target)
  if temporary.has_prefix("!") {
    raise @mcap.io_error(temporary)
  }
  let sink = create_sink(temporary)
  let key = file_key(temporary)
  if key.has_prefix("!") {
    ignore(remove_owned(temporary, false))
    raise @mcap.io_error(key)
  }
  { target, temporary, key, sink, closed: false, }
}

///|
pub fn OutputTransaction::publish(
  self : OutputTransaction,
) -> Unit raise @mcap.McapError {
  if self.closed {
    raise @mcap.io_error("output transaction is closed")
  }
  let error = publish_stage(
    self.temporary,
    self.target,
    self.key,
    self.sink.position().to_string(),
  )
  if error != "" {
    if error.has_prefix("!output published;") {
      self.closed = true
    }
    raise @mcap.io_error(error)
  }
  self.closed = true
}

///|
pub fn OutputTransaction::abort(
  self : OutputTransaction,
) -> Unit raise @mcap.McapError {
  if !self.closed {
    let error = remove_owned(self.temporary, false)
    if error != "" {
      raise @mcap.io_error(error)
    }
    self.closed = true
  }
}
