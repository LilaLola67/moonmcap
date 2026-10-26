///|
#external
type CacheHost

///|
extern "js" fn cache_open(
  url : String,
  directory : String,
  page : Int,
  capacity : Int,
  parallel : Int,
  ahead : Int,
  timeout : Int,
  retries : Int,
  signal : @js_async.AbortSignal,
) -> @js_async.Promise[CacheHost] =
  #| async (url,directory,page,capacity,parallel,ahead,timeout,retries,signal) => {
  #|   const fs=await import('node:fs/promises'),constants=(await import('node:fs')).constants;
  #|   const path=await import('node:path'),crypto=await import('node:crypto');
  #|   const fatal=m=>Object.assign(new Error(m),{permanent:true});
  #|   const hash=b=>crypto.createHash('sha256').update(b).digest();
  #|   const controller=new AbortController(),h={error:'',controller};
  #|   const root=path.resolve(directory),marker=path.join(root,'.moonmcap-cache-v1'),lease=path.join(root,'.moonmcap-cache-lock');
  #|   const signature='MoonMCAP HTTP cache v1\n',owned=/^[a-f0-9]{64}-[a-f0-9]{1,16}\.page$/,part=/^[a-f0-9]{64}-[a-f0-9]{24}\.part$/;
  #|   const stats={requests:0,response_bytes:0,disk_hits:0,disk_misses:0,corrupt_pages:0,evictions:0,cache_bytes:0,peak_disk_bytes:0,active_peak:0,pending_peak:0,prefetched_pages:0};
  #|   const pages=new Map(),pending=new Map(),queue=[]; let active=0,reads=0,serial=Promise.resolve(),closing=false,token='';
  #|   const mutate=f=>{const next=serial.then(f);serial=next.catch(()=>{});return next;};
  #|   async function regular(file,max) {
  #|     const fd=await fs.open(file,constants.O_RDONLY|(constants.O_NOFOLLOW||0));
  #|     try {const s=await fd.stat();if(!s.isFile()||s.nlink!==1||s.size>max)throw fatal('unsafe or oversized cache file');return await fd.readFile();} finally {await fd.close();}
  #|   }
  #|   async function release() {
  #|     if(!token)return;
  #|     const value=JSON.parse((await regular(lease,1024)).toString());
  #|     if(value.token!==token)throw fatal('cache directory lease changed');
  #|     await fs.unlink(lease);token='';
  #|   }
  #|   async function request(start,length,size='',etag='') {
  #|     const end=start+BigInt(length)-1n;
  #|     for(let attempt=0;attempt<=retries;attempt++) {
  #|       let response;
  #|       try {
  #|         if(controller.signal.aborted)throw fatal('cached HTTP source cancelled');
  #|         const headers={'Range':`bytes=${start}-${end}`,'Accept-Encoding':'identity'};if(etag)headers['If-Match']=etag;
  #|         stats.requests++;
  #|         response=await fetch(url,{headers,redirect:'error',signal:AbortSignal.any([controller.signal,AbortSignal.timeout(timeout)])});
  #|         if([408,429,500,502,503,504].includes(response.status))throw new Error('transient HTTP '+response.status);
  #|         if(response.status===412)throw fatal('HTTP source changed (412)');
  #|         if(response.status!==206)throw fatal('cached HTTP requires Range 206');
  #|         const encoding=response.headers.get('content-encoding');if(encoding&&encoding!=='identity')throw fatal('HTTP encoding must be identity');
  #|         const range=/^bytes (\d+)-(\d+)\/(\d+)$/.exec(response.headers.get('content-range')||'');
  #|         if(!range||BigInt(range[1])!==start||BigInt(range[2])!==end||BigInt(range[3])<=end||BigInt(range[3])>18446744073709551615n||(size&&range[3]!==size))throw fatal('invalid or changed HTTP Content-Range');
  #|         const tag=response.headers.get('etag')||'';if(!/^"[\x21\x23-\x7e\x80-\xff]*"$/.test(tag)||(etag&&tag!==etag))throw fatal('missing, weak or changed HTTP ETag');
  #|         const len=response.headers.get('content-length');if(len!==null&&(!/^\d+$/.test(len)||BigInt(len)!==BigInt(length)))throw fatal('HTTP response length mismatch');
  #|         if(!response.body)throw fatal('missing HTTP range body');
  #|         const reader=response.body.getReader(),chunks=[];let received=0;
  #|         try {for(;;){const {done,value}=await reader.read();if(done)break;received+=value.length;stats.response_bytes+=value.length;if(received>length)throw fatal('HTTP body exceeds range');chunks.push(value);}}finally{await reader.cancel().catch(()=>{});}
  #|         if(received!==length)throw new Error('truncated HTTP range body');
  #|         return {size:range[3],etag:tag,bytes:Buffer.concat(chunks,received)};
  #|       } catch(error) {
  #|         if(response?.body)await response.body.cancel().catch(()=>{});
  #|         if(error.permanent||controller.signal.aborted||attempt===retries)throw error;
  #|         await new Promise(resolve=>setTimeout(resolve,Math.min(25*2**attempt,250)));
  #|       }
  #|     }
  #|   }
  #|   try {
  #|     const parsed=new URL(url);if(!['http:','https:'].includes(parsed.protocol)||parsed.username||parsed.password)throw fatal('cache requires a credential-free http(s) URL');
  #|     if(signal.aborted)throw fatal('cache opening cancelled');
  #|     const abort=()=>controller.abort();signal.addEventListener('abort',abort,{once:true});
  #|     let meta;try{meta=await request(0n,1);}finally{signal.removeEventListener('abort',abort);}
  #|     h.size=meta.size;
  #|     const key=hash(JSON.stringify([url,meta.etag,meta.size,page])),keyHex=key.toString('hex');
  #|     try{await fs.mkdir(root);}catch(e){if(e.code!=='EEXIST')throw e;}
  #|     const directoryStat=await fs.lstat(root);if(!directoryStat.isDirectory()||directoryStat.isSymbolicLink())throw fatal('cache root must be a real directory');
  #|     try {
  #|       if((await regular(marker,128)).toString()!==signature)throw fatal('cache directory marker mismatch');
  #|     } catch(e) {
  #|       if(e.code!=='ENOENT')throw e;
  #|       const iter=await fs.opendir(root);let empty=true;for await(const entry of iter){empty=false;break;}
  #|       if(!empty)throw fatal('cache directory is not empty and has no ownership marker');
  #|       await fs.writeFile(marker,signature,{flag:'wx'});
  #|     }
  #|     try {
  #|       const previous=JSON.parse((await regular(lease,1024)).toString());
  #|       if(!Number.isSafeInteger(previous.pid)||previous.pid<1||!/^[a-f0-9]{32}$/.test(previous.token))throw fatal('invalid cache lease');
  #|       let alive=true;try{process.kill(previous.pid,0);}catch(e){if(e.code==='ESRCH')alive=false;else throw e;}
  #|       if(alive)throw fatal('cache directory is already in use');
  #|       await fs.unlink(lease);
  #|     } catch(e){if(e.code!=='ENOENT')throw e;}
  #|     token=crypto.randomBytes(16).toString('hex');
  #|     try{await fs.writeFile(lease,JSON.stringify({pid:process.pid,token}),{flag:'wx'});}catch(e){token='';throw e;}
  #|     let entries=0;
  #|     for await(const entry of await fs.opendir(root)) {
  #|       if(++entries>8192)throw fatal('cache directory entry budget exceeded');
  #|       if(entry.name==='.moonmcap-cache-v1'||entry.name==='.moonmcap-cache-lock')continue;
  #|       if(!owned.test(entry.name)&&!part.test(entry.name))throw fatal('unknown file in owned cache directory');
  #|       const file=path.join(root,entry.name),s=await fs.lstat(file);
  #|       if(!s.isFile()||s.isSymbolicLink()||s.nlink!==1)throw fatal('unsafe cache directory entry');
  #|       if(part.test(entry.name)){await fs.unlink(file);continue;}
  #|       if(s.size>4*1024*1024+84)throw fatal('oversized cache page');
  #|       pages.set(entry.name,{bytes:s.size,used:s.mtimeMs});stats.cache_bytes+=s.size;
  #|     }
  #|     async function evict(extra) {
  #|       while(stats.cache_bytes+extra>capacity||pages.size>=4096) {
  #|         let victim;for(const entry of pages){if(!victim||entry[1].used<victim[1].used)victim=entry;}
  #|         if(!victim)throw fatal('cache capacity cannot hold one page');
  #|         await fs.unlink(path.join(root,victim[0]));pages.delete(victim[0]);stats.cache_bytes-=victim[1].bytes;stats.evictions++;
  #|       }
  #|     }
  #|     await evict(0);stats.peak_disk_bytes=stats.cache_bytes;
  #|     async function load(offset,prefetched) {
  #|       const count=Number(BigInt(meta.size)-offset<BigInt(page)?BigInt(meta.size)-offset:BigInt(page));
  #|       const name=keyHex+'-'+offset.toString(16)+'.page',file=path.join(root,name);
  #|       const hit=await mutate(async()=>{
  #|         if(!pages.has(name))return null;
  #|         const entry=pages.get(name);let value;
  #|         try {
  #|           const data=await regular(file,page+84);
  #|           if(data.length!==count+84||data.subarray(0,8).toString('hex')!=='4d4843310d0a0000'||data.readBigUInt64LE(8)!==offset||data.readUInt32LE(16)!==count||!data.subarray(20,52).equals(key)||!data.subarray(52,84).equals(hash(data.subarray(84))))throw fatal('corrupt HTTP cache page');
  #|           value=data.subarray(84);
  #|         } catch(e) {
  #|           if(e.code==='ELOOP'||e.message==='unsafe or oversized cache file')throw e;
  #|           if(e.code!=='ENOENT')await fs.unlink(file);
  #|           pages.delete(name);stats.cache_bytes-=entry.bytes;stats.corrupt_pages++;return null;
  #|         }
  #|         entry.used=Date.now();await fs.utimes(file,new Date(),new Date());stats.disk_hits++;return value;
  #|       });
  #|       if(hit)return hit;
  #|       stats.disk_misses++;
  #|       const data=(await request(offset,count,meta.size,meta.etag)).bytes;
  #|       if(prefetched)stats.prefetched_pages++;
  #|       const envelope=Buffer.alloc(84);Buffer.from('4d4843310d0a0000','hex').copy(envelope);envelope.writeBigUInt64LE(offset,8);envelope.writeUInt32LE(count,16);key.copy(envelope,20);hash(data).copy(envelope,52);
  #|       await mutate(async()=>{
  #|         if(controller.signal.aborted)throw fatal('cached HTTP source cancelled');
  #|         await evict(data.length+84);
  #|         const temporary=path.join(root,keyHex+'-'+crypto.randomBytes(12).toString('hex')+'.part');
  #|         try {
  #|           await fs.writeFile(temporary,Buffer.concat([envelope,data]),{flag:'wx'});
  #|           stats.peak_disk_bytes=Math.max(stats.peak_disk_bytes,stats.cache_bytes+84+data.length);
  #|           await fs.rename(temporary,file);
  #|         } catch(e){await fs.unlink(temporary).catch(()=>{});throw e;}
  #|         pages.set(name,{bytes:84+data.length,used:Date.now()});stats.cache_bytes+=84+data.length;
  #|       });
  #|       return data;
  #|     }
  #|     function pump() {
  #|       while(active<parallel&&queue.length){const job=queue.shift();active++;stats.active_peak=Math.max(stats.active_peak,active);load(job.offset,job.prefetched).then(job.resolve,job.reject).finally(()=>{active--;pending.delete(job.name);pump();});}
  #|     }
  #|     function get(offset,prefetched) {
  #|       const name=offset.toString();if(pending.has(name))return pending.get(name);
  #|       if(pending.size>=64)return Promise.reject(fatal('cache pending-page budget exceeded'));
  #|       let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});pending.set(name,promise);queue.push({name,offset,prefetched,resolve,reject});stats.pending_peak=Math.max(stats.pending_peak,pending.size);pump();return promise;
  #|     }
  #|     h.read=async(offset,length,taskSignal)=>{
  #|       if(closing||controller.signal.aborted||taskSignal.aborted)return '!cached HTTP source cancelled';
  #|       if(reads>=16)return '!cache concurrent-read budget exceeded';reads++;
  #|       const start=BigInt(offset),base=start/BigInt(page)*BigInt(page),inside=Number(start-base),count=Math.min(length,page-inside);
  #|       let remove=()=>{};
  #|       try {
  #|         const wanted=get(base,false),batch=[wanted];
  #|         const extra=Math.min(ahead,Math.floor(capacity/(page+84))-1);
  #|         for(let i=1;i<=extra;i++){const next=base+BigInt(page*i);if(next>=BigInt(meta.size))break;batch.push(get(next,true));}
  #|         const cancelled=new Promise((_,reject)=>{const abort=()=>reject(fatal('cache read task cancelled'));taskSignal.addEventListener('abort',abort,{once:true});remove=()=>taskSignal.removeEventListener('abort',abort);});
  #|         const result=await Promise.race([Promise.all(batch),cancelled]);return result[0].subarray(inside,inside+count).toString('hex');
  #|       } catch(e){return '!'+e.message;}finally{remove();reads--;}
  #|     };
  #|     h.metrics=()=>JSON.stringify({...stats,closed:closing});
  #|     let closePromise;
  #|     h.close=()=>closePromise||(closePromise=(async()=>{
  #|       closing=true;controller.abort();
  #|       await Promise.allSettled([...pending.values()]);await serial;
  #|       try{await release();return '';}catch(e){return '!'+e.message;}
  #|     })());
  #|     if(signal.aborted)throw fatal('cache opening cancelled');
  #|     return h;
  #|   } catch(e) {
  #|     controller.abort();try{await release();}catch(_){}
  #|     h.error=e.message;return h;
  #|   }
  #| }

///|
extern "js" fn cache_error(host : CacheHost) -> String =
  #| h=>h.error

///|
extern "js" fn cache_size(host : CacheHost) -> String =
  #| h=>h.size

///|
extern "js" fn cache_stats(host : CacheHost) -> String =
  #| h=>h.metrics()

///|
extern "js" fn cache_abort(host : CacheHost) -> Unit =
  #| h=>h.controller.abort()

///|
extern "js" fn cache_close(host : CacheHost) -> @js_async.Promise[String] =
  #| h=>h.close()

///|
extern "js" fn cache_read(
  host : CacheHost,
  offset : String,
  length : Int,
  signal : @js_async.AbortSignal,
) -> @js_async.Promise[String] =
  #| (h,o,n,s)=>h.read(o,n,s)

///|
pub async fn open_cached_http(
  url : String,
  directory : String,
  options? : HttpCacheOptions = http_cache_options(),
) -> CachedHttpSource {
  if options.page_bytes < 4096 ||
    options.page_bytes > 4 * 1024 * 1024 ||
    options.cache_bytes < options.page_bytes + 84 ||
    options.cache_bytes > 1024 * 1024 * 1024 ||
    options.concurrency < 1 ||
    options.concurrency > 16 ||
    options.prefetch_pages < 0 ||
    options.prefetch_pages > 16 ||
    options.timeout_ms < 1 ||
    options.retries < 0 ||
    options.retries > 10 {
    raise @mcap.io_error("invalid HTTP cache budgets")
  }
  let host = @js_async.run_promise(fn(signal) {
    cache_open(
      url,
      directory,
      options.page_bytes,
      options.cache_bytes,
      options.concurrency,
      options.prefetch_pages,
      options.timeout_ms,
      options.retries,
      signal,
    )
  })
  if cache_error(host) != "" {
    raise @mcap.io_error(cache_error(host))
  }
  let source = new_source(
    @mcap.parse_decimal(cache_size(host)),
    async fn(offset, length) {
      let hex = @js_async.run_promise(fn(signal) {
        cache_read(host, offset.to_string(), length, signal)
      })
      if hex.has_prefix("!") {
        raise @mcap.io_error(hex)
      }
      @mcap.decode_hex(hex, max_bytes=options.page_bytes)
    },
    abort=fn() { cache_abort(host) },
  )
  {
    source,
    snapshot: fn() { cache_stats(host) },
    shutdown: async fn() {
      let error = @async.protect_from_cancel(async fn() {
        cache_close(host).wait()
      })
      if error != "" {
        raise @mcap.io_error(error)
      }
    },
  }
}
