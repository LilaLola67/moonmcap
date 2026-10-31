///|
extern "js" fn artifact_host(
  directory : String,
  resume : Bool,
) -> (String, Int32, Bytes) -> String =
  #| (directory,resume)=>{
  #| const fs=process.getBuiltinModule('node:fs'),p=process.getBuiltinModule('node:path'),crypto=process.getBuiltinModule('node:crypto');
  #| const path=p.resolve(directory),owner='MoonMCAP artifact batch 1\n',lock=p.join(path,'lock'),guard=p.join(path,'recovery'),handles=new Map();let key,closed=false;
  #| const regular=file=>{const s=fs.lstatSync(file);if(!s.isFile()||s.isSymbolicLink())throw new Error('artifact entry is not a regular file');return s;};
  #| const syncdir=()=>{let fd;try{fd=fs.openSync(path,'r');fs.fsyncSync(fd);}catch(e){if(process.platform!=='win32')throw e;}finally{if(fd!==undefined)fs.closeSync(fd);}};
  #| const owned=()=>{const s=fs.lstatSync(path);if(!s.isDirectory()||s.isSymbolicLink())throw new Error('artifact directory changed');regular(p.join(path,'owner'));if(fs.readFileSync(p.join(path,'owner'),'utf8')!==owner)throw new Error('artifact ownership mismatch');};
  #| const acquire=()=>{const fd=fs.openSync(lock,'wx');try{fs.writeFileSync(fd,JSON.stringify({pid:process.pid,key}));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}syncdir();};
  #| try{
  #|   if(!resume){fs.mkdirSync(path);fs.writeFileSync(p.join(path,'owner'),owner,{flag:'wx'});syncdir();}owned();key=crypto.randomUUID();if(fs.existsSync(guard))throw new Error('artifact recovery guard exists; inspect stopped task before removal');
  #|   if(fs.existsSync(lock)){
  #|     // Serialize stale-lock recovery. Ordinary wx acquisition may win the gap;
  #|     // in that case our acquisition fails without touching the new owner's lock.
  #|     fs.mkdirSync(guard);try{regular(lock);if(regular(lock).size>1024)throw new Error('oversized artifact lock');const old=JSON.parse(fs.readFileSync(lock,'utf8'));if(!Number.isInteger(old.pid)||old.pid<=0||typeof old.key!=='string')throw new Error('invalid artifact lock');let alive=true;try{process.kill(old.pid,0);}catch(e){if(e.code==='ESRCH')alive=false;else throw e;}if(alive)throw new Error('artifact batch locked by live process');fs.unlinkSync(lock);acquire();}finally{fs.rmdirSync(guard);}
  #|   }else acquire();
  #| }catch(e){return ()=> '!'+e.message;}
  #| const file=(id,suffix)=>p.join(path,`clip-${id}.${suffix}`);
  #| const inventory=()=>{const names=fs.readdirSync(path);for(const name of names){if(name==='recovery'){const s=fs.lstatSync(guard);if(!s.isDirectory()||s.isSymbolicLink())throw new Error('invalid artifact recovery guard');continue;}if(!['owner','lock','manifest.bin','manifest.tmp'].includes(name)&&!/^clip-(0|[1-9][0-9]{0,3})\.(part|mcap)$/.test(name))throw new Error('unrecognized artifact entry');regular(p.join(path,name));}return names;};
  #| const selected=id=>{const part=file(id,'part'),final=file(id,'mcap');if(fs.existsSync(part)&&fs.existsSync(final)){const a=regular(part),b=regular(final);if(a.ino!==b.ino||a.dev!==b.dev)throw new Error('conflicting artifact files');}const value=fs.existsSync(final)?final:part;regular(value);return value;};
  #| const closefd=id=>{if(handles.has(id)){const fd=handles.get(id);handles.delete(id);let error;try{fs.fsyncSync(fd);}catch(e){error=e;}try{fs.closeSync(fd);}catch(e){error=error?new Error(error.message+'; close cleanup: '+e.message):e;}if(error)throw error;}};
  #| const replace=(from,to)=>{for(let attempt=0;;attempt++){try{fs.renameSync(from,to);return;}catch(e){if(process.platform!=='win32'||!['EPERM','EACCES','EBUSY'].includes(e.code)||attempt>=7)throw e;Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,5*(attempt+1));}}};
  #| return (op,id,bytes)=>{try{
  #|   if(closed)throw new Error('artifact directory closed');owned();regular(lock);if(JSON.parse(fs.readFileSync(lock,'utf8')).key!==key)throw new Error('artifact lock ownership changed');
  #|   if(op==='unlock'){for(const n of handles.keys())closefd(n);fs.unlinkSync(lock);syncdir();closed=true;return '';}
  #|   const names=inventory();if(!Number.isInteger(id)||id<0||id>=4096)throw new Error('invalid artifact ordinal');
  #|   if(op==='load'){const manifest=p.join(path,'manifest.bin');if(!fs.existsSync(manifest))return '';if(regular(manifest).size>8388608)throw new Error('artifact manifest exceeds budget');return fs.readFileSync(manifest).toString('hex');}
  #|   if(op==='save'){if(bytes.length>8388608)throw new Error('artifact manifest exceeds budget');const temp=p.join(path,'manifest.tmp');if(fs.existsSync(temp))regular(temp);const fd=fs.openSync(temp,'w');try{fs.writeFileSync(fd,bytes);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}replace(temp,p.join(path,'manifest.bin'));syncdir();return '';}
  #|   if(op==='list')return JSON.stringify([...new Set(names.filter(n=>/^clip-\d+\.(part|mcap)$/.test(n)).map(n=>Number(n.split('-')[1].split('.')[0])))].sort((a,b)=>a-b));
  #|   if(op==='published')return String(fs.existsSync(file(id,'mcap')));
  #|   if(op==='source')return selected(id);
  #|   if(op==='create'){if(fs.existsSync(file(id,'mcap'))||handles.has(id))throw new Error('artifact ordinal already published or open');handles.set(id,fs.openSync(file(id,'part'),'wx'));syncdir();return '';}
  #|   if(op==='write'){if(!handles.has(id))throw new Error('artifact is not owned by current writer');let done=0;while(done<bytes.length){const n=fs.writeSync(handles.get(id),bytes,done,bytes.length-done);if(!n)throw new Error('artifact write made no progress');done+=n;}return '';}
  #|   if(op==='seal'){closefd(id);const fd=fs.openSync(selected(id),'r+');try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}syncdir();return '';}
  #|   if(op==='publish'){closefd(id);const part=file(id,'part'),final=file(id,'mcap');selected(id);if(!fs.existsSync(final))fs.linkSync(part,final);if(fs.existsSync(part))fs.unlinkSync(part);syncdir();return '';}
  #|   if(op==='discard'){if(fs.existsSync(file(id,'mcap')))throw new Error('cannot discard published artifact');closefd(id);const part=file(id,'part');if(fs.existsSync(part)){regular(part);fs.unlinkSync(part);syncdir();}return '';}
  #|   throw new Error('unknown artifact directory operation');
  #| }catch(e){return '!'+e.message;}};
  #| }

///|
pub fn open_artifact_directory(
  path : String,
  resume~ : Bool = false,
) -> ArtifactDirectory raise @mcap.McapError {
  let directory : ArtifactDirectory = {
    operate: artifact_host(path, resume),
    closed: false,
  }
  ignore(
    directory.operation("list") catch {
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
