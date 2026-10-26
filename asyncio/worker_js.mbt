///|
#external
type WorkerHost

///|
extern "js" fn worker_open(
  path : String,
  workers : Int,
  max_jobs : Int,
  input : Int,
  output : Int,
  context : Int,
  budget : Int,
  timeout : Int,
  signal : @js_async.AbortSignal,
) -> @js_async.Promise[WorkerHost] =
  #| async(path,count,maxJobs,input,output,context,budget,timeout,signal)=>{
  #| const {Worker}=await import('node:worker_threads'),fs=await import('node:fs/promises'),p=await import('node:path');
  #| const h={error:''},slots=[],jobs=new Map(),queue=[],threads=new Set();let closed=false,failure='',serial=0,closePromise;
  #| const stats={submitted:0,completed:0,failed:0,cancelled:0,active:0,active_peak:0,pending_peak:0,reserved_bytes:0,peak_reserved_bytes:0};
  #| function finish(job,result){if(!jobs.has(job.id))return;jobs.delete(job.id);stats.reserved_bytes-=job.reserve;job.signal.removeEventListener('abort',job.abort);if(job.timer)clearTimeout(job.timer);if(!job.cancelled){if(result.startsWith('!'))stats.failed++;else stats.completed++;}job.resolve(result);}
  #| async function stop(){if(closePromise)return closePromise;closed=true;closePromise=(async()=>{for(const job of [...jobs.values()])finish(job,'!worker pool closed');queue.length=0;for(const slot of slots)slot.job=null;await Promise.allSettled(slots.map(s=>s.worker.terminate()));stats.active=0;})();return closePromise;}
  #| function fail(message){if(failure)return;failure=message;for(const job of [...jobs.values()])finish(job,'!'+message);void stop();}
  #| function pump(){if(closed||failure)return;for(const slot of slots){if(slot.job||!slot.ready)continue;let job;while(queue.length&&!job){const candidate=queue.shift();if(jobs.has(candidate.id))job=candidate;}if(!job)break;slot.job=job;job.running=true;stats.active++;stats.active_peak=Math.max(stats.active_peak,stats.active);job.timer=setTimeout(()=>fail('codec worker deadline exceeded'),timeout);slot.worker.postMessage({id:job.id,payload:job.payload});}}
  #| try {
  #| const resolved=p.resolve(path),stat=await fs.stat(resolved);if(!stat.isFile()||stat.size>16*1024*1024)throw new Error('worker entry must be a bounded compiled local JavaScript file');
  #| if(signal.aborted)throw new Error('worker opening cancelled');
  #| const ready=[];
  #| for(let i=0;i<count;i++){
  #| const slot={worker:new Worker(resolved,{workerData:{moonmcap:{input,output,context}}}),ready:false,job:null};slots.push(slot);
  #| ready.push(new Promise((resolve,reject)=>{
  #| const timer=setTimeout(()=>reject(new Error('codec worker startup deadline exceeded')),timeout);
  #| slot.worker.on('message',message=>{
  #| if(closed)return;
  #| if(message?.ready===true&&!slot.ready){clearTimeout(timer);slot.ready=true;resolve();pump();return;}
  #| const job=slot.job;if(!job||message?.id!==job.id||typeof message.result!=='string'){fail('invalid codec worker response envelope');return;}
  #| const result=message.result;if((result.startsWith('!')&&result.length>4096)||(!result.startsWith('!')&&(result.length>2*job.maximum||result.length%2!==0||/[^0-9a-f]/.test(result)))){fail('codec worker response exceeds budget or is malformed');return;}
  #| if(Number.isInteger(message.thread))threads.add(message.thread);slot.job=null;stats.active--;finish(job,result);pump();
  #| });
  #| slot.worker.on('error',e=>{clearTimeout(timer);reject(e);fail('codec worker failed: '+e.message);});
  #| slot.worker.on('exit',code=>{clearTimeout(timer);if(!closed){reject(new Error('codec worker exited'));fail('codec worker exited '+code);}});
  #| }));}
  #| const cancelled=new Promise((_,reject)=>{h.openAbort=()=>reject(new Error('worker opening cancelled'));signal.addEventListener('abort',h.openAbort,{once:true});});
  #| try{await Promise.race([Promise.all(ready),cancelled]);}finally{signal.removeEventListener('abort',h.openAbort);}
  #| h.submit=(payload,maximum,taskSignal)=>new Promise(resolve=>{
  #| if(closed||failure){resolve('!'+(failure||'worker pool closed'));return;}
  #| const bytes=Buffer.byteLength(payload),reserve=bytes+2*maximum;
  #| if(taskSignal.aborted){resolve('!worker task cancelled');return;}
  #| if(maximum<0||maximum>output||bytes>2*(input+context)+32768||jobs.size>=maxJobs||reserve>budget-stats.reserved_bytes){resolve('!worker job or encoded buffer budget exceeded');return;}
  #| const job={id:++serial,payload,maximum,reserve,signal:taskSignal,resolve,timer:null,cancelled:false,running:false};
  #| job.abort=()=>{if(!jobs.has(job.id)||job.cancelled)return;stats.cancelled++;job.cancelled=true;if(job.running){taskSignal.removeEventListener('abort',job.abort);resolve('!worker task cancelled');}else{const at=queue.indexOf(job);if(at>=0)queue.splice(at,1);finish(job,'!worker task cancelled');}};
  #| taskSignal.addEventListener('abort',job.abort,{once:true});jobs.set(job.id,job);stats.submitted++;stats.reserved_bytes+=reserve;stats.peak_reserved_bytes=Math.max(stats.peak_reserved_bytes,stats.reserved_bytes);stats.pending_peak=Math.max(stats.pending_peak,jobs.size);queue.push(job);pump();
  #| });
  #| h.metrics=()=>JSON.stringify({...stats,threads:[...threads],workers:count,closed,failure:!!failure});h.close=stop;return h;
  #| }catch(e){h.error=e.message;await stop();return h;}
  #| }

///|
extern "js" fn worker_error(host : WorkerHost) -> String =
  #| h=>h.error

///|
extern "js" fn worker_stats(host : WorkerHost) -> String =
  #| h=>h.metrics()

///|
extern "js" fn worker_submit(
  host : WorkerHost,
  job : String,
  maximum : Int,
  signal : @js_async.AbortSignal,
) -> @js_async.Promise[String] =
  #| (h,j,m,s)=>h.submit(j,m,s)

///|
extern "js" fn worker_close(host : WorkerHost) -> @js_async.Promise[Unit] =
  #| h=>h.close()

///|
extern "js" fn worker_register(callback : (String) -> String) -> String =
  #| callback=>{const {parentPort,threadId}=process.getBuiltinModule('node:worker_threads');if(!parentPort)return 'codec entry must run in a worker thread';parentPort.on('message',m=>{try{parentPort.postMessage({id:m.id,result:callback(m.payload),thread:threadId});}catch(e){parentPort.postMessage({id:m.id,result:'!'+e.message,thread:threadId});}});parentPort.postMessage({ready:true});return '';}

///|
extern "js" fn worker_config(key : String, fallback : Int) -> Int =
  #| (key,fallback)=>process.getBuiltinModule('node:worker_threads').workerData?.moonmcap?.[key]??fallback

///|
pub fn run_codec_worker() -> Unit raise @mcap.McapError {
  let callback = @mcap.codec_worker_handler(
    input_bytes=worker_config("input", 32 * 1024 * 1024),
    output_bytes=worker_config("output", 128 * 1024 * 1024),
    context_bytes=worker_config("context", 1024 * 1024),
  )
  let error = worker_register(callback)
  if error != "" {
    raise @mcap.io_error(error)
  }
}

///|
pub async fn open_worker_pool(
  path : String,
  options? : WorkerOptions = worker_options(),
) -> WorkerPool {
  if options.workers < 1 ||
    options.workers > 8 ||
    options.max_jobs < 1 ||
    options.max_jobs > 64 ||
    options.input_bytes < 1 ||
    options.input_bytes > 128 * 1024 * 1024 ||
    options.output_bytes < 40 ||
    options.output_bytes > 128 * 1024 * 1024 ||
    options.context_bytes < 1 ||
    options.context_bytes > 16 * 1024 * 1024 ||
    options.buffer_bytes < 1024 ||
    options.buffer_bytes > 1024 * 1024 * 1024 ||
    options.prefetch < 1 ||
    options.prefetch > 16 ||
    options.min_chunk_bytes < 0 ||
    options.min_payload_bytes < 0 ||
    options.timeout_ms < 1 {
    raise @mcap.io_error("invalid codec worker options")
  }
  let host = @js_async.run_promise(fn(signal) {
    worker_open(
      path,
      options.workers,
      options.max_jobs,
      options.input_bytes,
      options.output_bytes,
      options.context_bytes,
      options.buffer_bytes,
      options.timeout_ms,
      signal,
    )
  })
  if worker_error(host) != "" {
    raise @mcap.io_error(worker_error(host))
  }
  {
    options,
    submit: async fn(job, maximum) {
      @js_async.run_promise(fn(signal) {
        worker_submit(host, job, maximum, signal)
      })
    },
    snapshot: fn() { worker_stats(host) },
    shutdown: async fn() { worker_close(host).wait() },
  }
}
