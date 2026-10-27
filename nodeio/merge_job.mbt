///|
extern "js" fn job_open(path : String, resume : Bool) -> String =
  #| (path,resume)=>{const fs=process.getBuiltinModule('node:fs'),p=process.getBuiltinModule('node:path'),crypto=process.getBuiltinModule('node:crypto');try{
  #|   if(!resume){fs.mkdirSync(path);fs.writeFileSync(p.join(path,'owner.json'),'MoonMCAP merge job 1\n',{flag:'wx'});}
  #|   if(!fs.lstatSync(path).isDirectory()||fs.lstatSync(path).isSymbolicLink()||fs.readFileSync(p.join(path,'owner.json'),'utf8')!=='MoonMCAP merge job 1\n')throw new Error('not an owned merge job');
  #|   const lock=p.join(path,'lock');
  #|   if(fs.existsSync(lock)){const s=fs.lstatSync(lock);if(!s.isFile()||s.isSymbolicLink()||s.size>1024)throw new Error('invalid job lock');const old=JSON.parse(fs.readFileSync(lock,'utf8'));if(!Number.isInteger(old.pid)||old.pid<=0)throw new Error('invalid lock owner');let alive=true;try{process.kill(old.pid,0);}catch(e){if(e.code==='ESRCH')alive=false;else throw e;}if(alive)throw new Error('merge job is locked by a live process');fs.unlinkSync(lock);}
  #|   const key=crypto.randomUUID();fs.writeFileSync(lock,JSON.stringify({pid:process.pid,key}),{flag:'wx'});return key;
  #| }catch(e){return '!'+e.message;}}

///|
extern "js" fn job_operation(
  path : String,
  key : String,
  op : String,
  payload : String,
) -> String =
  #| (path,key,op,payload)=>{const fs=process.getBuiltinModule('node:fs'),p=process.getBuiltinModule('node:path');try{
  #|   if(!fs.lstatSync(path).isDirectory()||fs.lstatSync(path).isSymbolicLink()||fs.readFileSync(p.join(path,'owner.json'),'utf8')!=='MoonMCAP merge job 1\n')throw new Error('job ownership changed');
  #|   const lock=p.join(path,'lock');if(JSON.parse(fs.readFileSync(lock,'utf8')).key!==key)throw new Error('job lock ownership changed');
  #|   const names=fs.readdirSync(path);for(const name of names){if(!/^(?:-(?:[1-9]|[1-5][0-9]|6[0-5])|0|[1-9][0-9]*)\.run$/.test(name)&&!['owner.json','lock','checkpoint.bin','checkpoint.tmp','result.part','complete.json'].includes(name))throw new Error('unrecognized file in job directory');const s=fs.lstatSync(p.join(path,name));if(!s.isFile()||s.isSymbolicLink())throw new Error('job contains nonregular entry');}
  #|   const syncdir=()=>{let fd;try{fd=fs.openSync(path,'r');fs.fsyncSync(fd);}catch(e){if(process.platform!=='win32')throw e;}finally{if(fd!==undefined)fs.closeSync(fd);}};
  #|   const checkpoint=p.join(path,'checkpoint.bin');
  #|   const replace=(from,to)=>{for(let attempt=0;;attempt++){try{fs.renameSync(from,to);return;}catch(e){if(process.platform!=='win32'||!['EPERM','EACCES','EBUSY'].includes(e.code)||attempt>=7)throw e;Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,5*(attempt+1));}}};
  #|   if(op==='load'){if(fs.existsSync(p.join(path,'complete.json')))throw new Error('merge job already published');if(!fs.existsSync(checkpoint))return '';if(fs.statSync(checkpoint).size>8388608)throw new Error('checkpoint exceeds budget');return fs.readFileSync(checkpoint).toString('hex');}
  #|   if(op==='prune'){const ids=new Set(JSON.parse(payload).map(String));for(const name of names){if((name.endsWith('.run')&&!ids.has(name.slice(0,-4)))||name==='checkpoint.tmp'||name==='result.part')fs.unlinkSync(p.join(path,name));}return '';}
  #|   if(op==='save'){const data=Buffer.from(payload,'hex');if(data.length>8388608)throw new Error('checkpoint exceeds budget');for(const name of names){if(name.endsWith('.run')){const fd=fs.openSync(p.join(path,name),'r+');try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}}}const tmp=p.join(path,'checkpoint.tmp');const fd=fs.openSync(tmp,'w');try{fs.writeFileSync(fd,data);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}replace(tmp,checkpoint);syncdir();return '';}
  #|   if(op==='complete'){fs.writeFileSync(p.join(path,'complete.json'),payload,{flag:'wx'});return '';}
  #|   if(op==='inspect'){let bytes=0;for(const name of names)bytes+=fs.statSync(p.join(path,name)).size;return JSON.stringify({checkpoint:fs.existsSync(checkpoint),published:fs.existsSync(p.join(path,'complete.json')),run_files:names.filter(n=>n.endsWith('.run')).length,bytes:String(bytes)});}
  #|   if(op==='clean'){for(const name of names)if(name!=='lock'&&name!=='owner.json')fs.unlinkSync(p.join(path,name));fs.unlinkSync(p.join(path,'owner.json'));fs.unlinkSync(lock);fs.rmdirSync(path);return '';}
  #|   if(op==='unlock'){fs.unlinkSync(lock);return '';}
  #|   throw new Error('unknown job operation');
  #| }catch(e){return '!'+e.message;}}

///|
pub fn open_merge_job(
  path : String,
  resume? : Bool = false,
) -> MergeJob raise @mcap.McapError {
  let key = job_open(path, resume)
  if key.has_prefix("!") {
    raise @mcap.io_error(key)
  }
  { path, key, closed: false, }
}

///|
fn MergeJob::operation(
  self : MergeJob,
  op : String,
  payload : String,
) -> String raise @mcap.McapError {
  if self.closed {
    raise @mcap.io_error("merge job is closed")
  }
  let result = job_operation(self.path, self.key, op, payload)
  if result.has_prefix("!") {
    raise @mcap.io_error(result)
  }
  result
}

///|
pub fn MergeJob::checkpoint(self : MergeJob) -> Bytes? raise @mcap.McapError {
  let hex = self.operation("load", "")
  let checkpoint = if hex == "" {
    None
  } else {
    Some(@mcap.decode_hex(hex, max_bytes=8 * 1024 * 1024))
  }
  let ids = match checkpoint {
    None => []
    Some(bytes) => @mcap.checkpoint_run_ids(bytes)
  }
  let json = Json::array(ids.map(fn(id) { Json::number(id.to_double()) }))
  ignore(self.operation("prune", json.stringify()))
  checkpoint
}

///|
pub fn MergeJob::save(
  self : MergeJob,
  bytes : Bytes,
) -> Unit raise @mcap.McapError {
  ignore(self.operation("save", @mcap.encode_hex(bytes)))
}

///|
pub fn MergeJob::storage(self : MergeJob) -> @mcap.SortStorage {
  @mcap.sort_storage(
    fn(id) {
      ignore(self.operation("inspect", ""))
      create_sink("\{self.path}/\{id}.run")
    },
    fn(id) {
      ignore(self.operation("inspect", ""))
      open_file("\{self.path}/\{id}.run")
    },
    fn(id) {
      ignore(self.operation("inspect", ""))
      let error = remove_owned("\{self.path}/\{id}.run", false)
      if error != "" {
        raise @mcap.io_error(error)
      }
    },
  )
}

///|
pub fn MergeJob::output(
  self : MergeJob,
  target : String,
) -> OutputTransaction raise @mcap.McapError {
  ignore(self.operation("inspect", ""))
  let check = stage_path(target)
  if check.has_prefix("!") {
    raise @mcap.io_error(check)
  }
  let temporary = "\{self.path}/result.part"
  let sink = create_sink(temporary)
  let key = file_key(temporary)
  if key.has_prefix("!") {
    raise @mcap.io_error(key)
  }
  { target, temporary, key, sink, closed: false, }
}

///|
pub fn MergeJob::complete(
  self : MergeJob,
  report : Json,
) -> Unit raise @mcap.McapError {
  ignore(self.operation("complete", report.stringify()))
}

///|
pub fn MergeJob::inspect(self : MergeJob) -> String raise @mcap.McapError {
  let value = @json.parse(self.operation("inspect", "")[:]) catch {
    _ => raise @mcap.io_error("invalid job status")
  }
  if value is Object(fields) && fields.get("checkpoint") is Some(True) {
    fields["progress"] = @mcap.checkpoint_progress(
      read_file("\{self.path}/checkpoint.bin", maximum=8 * 1024 * 1024),
    )
  }
  value.stringify()
}

///|
pub fn MergeJob::close(self : MergeJob) -> Unit raise @mcap.McapError {
  if !self.closed {
    ignore(self.operation("unlock", ""))
    self.closed = true
  }
}

///|
pub fn MergeJob::clean(self : MergeJob) -> Unit raise @mcap.McapError {
  ignore(self.operation("clean", ""))
  self.closed = true
}
