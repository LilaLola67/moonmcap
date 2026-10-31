///|
extern "js" fn recording_host(
  path : String,
  resume : Bool,
) -> (String, Int32, Bytes) -> String =
  #| (directory,resume)=>{
  #| const fs=process.getBuiltinModule('node:fs'),p=process.getBuiltinModule('node:path'),crypto=process.getBuiltinModule('node:crypto');
  #| const path=p.resolve(directory),owner='MoonMCAP recording session 1\n',handles=new Map();let key,closed=false;
  #| const syncdir=()=>{let fd;try{fd=fs.openSync(path,'r');fs.fsyncSync(fd);}catch(e){if(process.platform!=='win32')throw e;}finally{if(fd!==undefined)fs.closeSync(fd);}};
  #| const regular=file=>{const s=fs.lstatSync(file);if(!s.isFile()||s.isSymbolicLink())throw new Error('recording entry is not a regular file');return s;};
  #| const owned=()=>{const s=fs.lstatSync(path);if(!s.isDirectory()||s.isSymbolicLink())throw new Error('recording directory changed');regular(p.join(path,'owner'));if(fs.readFileSync(p.join(path,'owner'),'utf8')!==owner)throw new Error('recording ownership mismatch');};
  #| const lock=p.join(path,'lock');
  #| try{
  #|   if(!resume){fs.mkdirSync(path);fs.writeFileSync(p.join(path,'owner'),owner,{flag:'wx'});syncdir();}
  #|   owned();
  #|   if(fs.existsSync(lock)){regular(lock);if(fs.statSync(lock).size>1024)throw new Error('oversized recording lock');const old=JSON.parse(fs.readFileSync(lock,'utf8'));if(!Number.isInteger(old.pid)||old.pid<=0)throw new Error('invalid recording lock');let alive=true;try{process.kill(old.pid,0);}catch(e){if(e.code==='ESRCH')alive=false;else throw e;}if(alive)throw new Error('recording session locked by live process');fs.unlinkSync(lock);}
  #|   key=crypto.randomUUID();const fd=fs.openSync(lock,'wx');try{fs.writeFileSync(fd,JSON.stringify({pid:process.pid,key}));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}syncdir();
  #| }catch(e){return ()=> '!'+e.message;}
  #| const segment=(id,suffix)=>p.join(path,`segment-${id}.${suffix}`);
  #| const inventory=()=>{const names=fs.readdirSync(path);for(const name of names){if(!['owner','lock','manifest.bin','manifest.tmp'].includes(name)&&!/^segment-(0|[1-9][0-9]{0,3})\.(part|mcap|quarantine)$/.test(name))throw new Error('unrecognized recording entry');regular(p.join(path,name));}return names;};
  #| const select=id=>{const part=segment(id,'part'),final=segment(id,'mcap');if(fs.existsSync(part)&&fs.existsSync(final)){const a=regular(part),b=regular(final);if(a.ino!==b.ino||a.dev!==b.dev)throw new Error('conflicting recording segment files');}return fs.existsSync(final)?final:part;};
  #| const closefd=id=>{if(handles.has(id)){const fd=handles.get(id);handles.delete(id);let error;try{fs.fsyncSync(fd);}catch(e){error=e;}try{fs.closeSync(fd);}catch(e){error=error?new Error(error.message+'; close cleanup: '+e.message):e;}if(error)throw error;}};
  #| const replace=(from,to)=>{for(let attempt=0;;attempt++){try{fs.renameSync(from,to);return;}catch(e){if(process.platform!=='win32'||!['EPERM','EACCES','EBUSY'].includes(e.code)||attempt>=7)throw e;Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,5*(attempt+1));}}};
  #| return (op,id,bytes)=>{try{
  #|   if(closed)throw new Error('recording directory closed');owned();regular(lock);if(JSON.parse(fs.readFileSync(lock,'utf8')).key!==key)throw new Error('recording lock ownership changed');
  #|   if(op==='unlock'){for(const n of handles.keys())closefd(n);fs.unlinkSync(lock);syncdir();closed=true;return '';}
  #|   const names=inventory();if(!Number.isInteger(id)||id<0||id>=4096)throw new Error('invalid recording ordinal');
  #|   if(op==='load'){const file=p.join(path,'manifest.bin');if(!fs.existsSync(file))return '';if(regular(file).size>8388608)throw new Error('recording manifest exceeds budget');return fs.readFileSync(file).toString('hex');}
  #|   if(op==='save'){if(bytes.length>8388608)throw new Error('recording manifest exceeds budget');const file=p.join(path,'manifest.tmp');const fd=fs.openSync(file,'w');try{fs.writeFileSync(fd,bytes);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}replace(file,p.join(path,'manifest.bin'));syncdir();return '';}
  #|   if(op==='list')return JSON.stringify([...new Set(names.filter(n=>/^segment-\d+\.(part|mcap)$/.test(n)).map(n=>Number(n.split('-')[1].split('.')[0])))].sort((a,b)=>a-b));
  #|   if(op==='inspect')return JSON.stringify(names.filter(n=>n!=='lock').map(n=>({name:n,bytes:String(regular(p.join(path,n)).size)})));
  #|   if(op==='exists')return String(fs.existsSync(segment(id,'part'))||fs.existsSync(segment(id,'mcap')));
  #|   if(op==='source'){const file=select(id);regular(file);return file;}
  #|   if(op==='create'){if(fs.existsSync(segment(id,'mcap'))||fs.existsSync(segment(id,'quarantine')))throw new Error('recording ordinal already used');handles.set(id,fs.openSync(segment(id,'part'),'wx'));syncdir();return '';}
  #|   if(op==='write'){if(!handles.has(id))throw new Error('recording segment is not owned by current writer');const fd=handles.get(id);let done=0;while(done<bytes.length){const n=fs.writeSync(fd,bytes,done,bytes.length-done);if(!n)throw new Error('recording write made no progress');done+=n;}return '';}
  #|   if(op==='seal'){closefd(id);const part=segment(id,'part'),final=segment(id,'mcap');const file=select(id);regular(file);const fd=fs.openSync(file,'r+');try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}if(!fs.existsSync(final))fs.linkSync(part,final);if(fs.existsSync(part))fs.unlinkSync(part);syncdir();return '';}
  #|   if(op==='quarantine'){closefd(id);const file=select(id),target=segment(id,'quarantine');const a=regular(file);if(fs.existsSync(target)){const b=regular(target);if(a.ino!==b.ino||a.dev!==b.dev)throw new Error('quarantine target conflicts');}else fs.linkSync(file,target);for(const suffix of ['part','mcap'])if(fs.existsSync(segment(id,suffix)))fs.unlinkSync(segment(id,suffix));syncdir();return '';}
  #|   throw new Error('unknown recording directory operation');
  #| }catch(e){return '!'+e.message;}};
  #| }

///|
pub fn open_recording_directory(
  path : String,
  resume~ : Bool = false,
) -> RecordingDirectory raise @mcap.McapError {
  let directory : RecordingDirectory = {
    operate: recording_host(path, resume),
    closed: false,
  }
  ignore(
    directory.operation("inspect") catch {
      error => {
        directory.close() catch {
          _ => ()
        }
        raise error
      }
    },
  )
  directory
}
