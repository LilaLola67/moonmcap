///|
extern "js" fn http_range(
  url : String,
  offset : String,
  length : Int,
  expected_size : String,
  expected_etag : String,
  timeout_ms : Int,
  retries : Int,
  task_signal : @js_async.AbortSignal,
  source_signal : @js_async.AbortSignal,
) -> @js_async.Promise[String] =
  #| async (url,offset,length,expectedSize,expectedEtag,timeoutMs,retries,taskSignal,sourceSignal) => {
  #|   const fatal = message => Object.assign(new Error(message),{permanent:true});
  #|   try { const u=new URL(url); if(!['http:','https:'].includes(u.protocol)||u.username||u.password) return '!HTTP requires a credential-free http(s) URL'; } catch { return '!invalid HTTP URL'; }
  #|   const start=BigInt(offset),end=start+BigInt(length)-1n;
  #|   for(let attempt=0;attempt<=retries;attempt++) {
  #|     let response;
  #|     try {
  #|       if(taskSignal.aborted||sourceSignal.aborted) throw fatal('HTTP read cancelled');
  #|       const signal=AbortSignal.any([taskSignal,sourceSignal,AbortSignal.timeout(timeoutMs)]);
  #|       const headers={'Range':`bytes=${start}-${end}`,'Accept-Encoding':'identity'};
  #|       if(expectedEtag) headers['If-Match']=expectedEtag;
  #|       response=await fetch(url,{headers,signal,redirect:'error'});
  #|       if([408,429,500,502,503,504].includes(response.status)) throw new Error('transient HTTP '+response.status);
  #|       if(response.status===412) throw fatal('HTTP source changed (412)');
  #|       if(response.status!==206) throw fatal('server did not return HTTP 206 for Range');
  #|       const encoding=response.headers.get('content-encoding');
  #|       if(encoding&&encoding!=='identity') throw fatal('HTTP representation must use identity content encoding');
  #|       const range=/^bytes (\d+)-(\d+)\/(\d+)$/.exec(response.headers.get('content-range')||'');
  #|       if(!range||BigInt(range[1])!==start||BigInt(range[2])!==end||BigInt(range[3])<=end) throw fatal('invalid HTTP Content-Range');
  #|       if(expectedSize&&range[3]!==expectedSize) throw fatal('HTTP source size changed');
  #|       const etag=response.headers.get('etag')||'';
  #|       if(!/^"[\x21\x23-\x7e\x80-\xff]*"$/.test(etag)) throw fatal('HTTP source requires a strong ETag');
  #|       if(expectedEtag&&etag!==expectedEtag) throw fatal('HTTP source ETag changed');
  #|       const contentLength=response.headers.get('content-length');
  #|       if(contentLength!==null&&(!/^\d+$/.test(contentLength)||BigInt(contentLength)!==BigInt(length))) throw fatal('HTTP response length mismatch');
  #|       if(!response.body) throw fatal('missing HTTP body');
  #|       const reader=response.body.getReader(),parts=[]; let received=0;
  #|       try { for(;;) {const {done,value}=await reader.read();if(done)break;received+=value.length;if(received>length)throw fatal('HTTP body exceeds requested range');parts.push(value);} }
  #|       finally { await reader.cancel().catch(()=>{}); }
  #|       if(received!==length) throw new Error('truncated HTTP body');
  #|       if(!expectedSize) return range[3]+'|'+etag;
  #|       return Buffer.concat(parts,received).toString('hex');
  #|     } catch(error) {
  #|       if(response?.body) await response.body.cancel().catch(()=>{});
  #|       if(error.permanent||taskSignal.aborted||sourceSignal.aborted||attempt===retries) return '!'+error.message;
  #|       await new Promise(resolve=>setTimeout(resolve,Math.min(25*2**attempt,250)));
  #|     }
  #|   }
  #|   return '!HTTP retry budget exhausted';
  #| }

///|
pub async fn open_http(
  url : String,
  timeout_ms? : Int = 5000,
  retries? : Int = 2,
  max_range_bytes? : Int = 32 * 1024 * 1024,
) -> AsyncSource {
  if timeout_ms <= 0 || retries < 0 || retries > 10 || max_range_bytes <= 0 {
    raise @mcap.io_error("invalid HTTP timeout, retry or range budget")
  }
  let controller = @js_async.AbortController::new()
  let metadata = @js_async.run_promise(fn(signal) {
    http_range(
      url,
      "0",
      1,
      "",
      "",
      timeout_ms,
      retries,
      signal,
      controller.signal(),
    )
  })
  if metadata.has_prefix("!") {
    raise @mcap.io_error(metadata)
  }
  let (size, etag) = match metadata.split_once("|") {
    Some((size, etag)) =>
      (@mcap.parse_decimal(size.to_string()), etag.to_string())
    None => raise @mcap.io_error("invalid HTTP metadata")
  }
  new_source(
    size,
    async fn(offset, length) {
      let count = length.min(max_range_bytes)
      let hex = @js_async.run_promise(fn(signal) {
        http_range(
          url,
          offset.to_string(),
          count,
          size.to_string(),
          etag,
          timeout_ms,
          retries,
          signal,
          controller.signal(),
        )
      })
      if hex.has_prefix("!") {
        raise @mcap.io_error(hex)
      }
      @mcap.decode_hex(hex, max_bytes=count)
    },
    abort=fn() { controller.abort() },
  )
}
