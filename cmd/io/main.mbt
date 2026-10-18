///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn clock_ms() -> Double =
  #| ()=>performance.now()

///|
extern "js" fn rss_kib() -> Int =
  #| ()=>process.resourceUsage().maxRSS

///|
extern "js" fn report_error(message : String) -> Unit =
  #| message=>{process.stderr.write(message+'\n');process.exitCode=2;}

///|
// Retained solely as a reproducible pre-0.15.6 bridge baseline, not a library API.
extern "js" fn legacy_read(
  path : String,
  offset : String,
  length : Int,
) -> String =
  #| (path,offset,length)=>{const fs=process.getBuiltinModule('node:fs');let fd;try{fd=fs.openSync(path,'r');const before=fs.fstatSync(fd,{bigint:true});const sig=s=>`${s.size}|${s.dev}|${s.ino}|${s.mtimeNs}|${s.ctimeNs}`;const data=Buffer.alloc(length),n=fs.readSync(fd,data,0,length,Number(BigInt(offset)));if(sig(before)!==sig(fs.fstatSync(fd,{bigint:true})))throw new Error('source changed');return data.subarray(0,n).toString('hex');}catch(e){return '!'+e.message;}finally{if(fd!==undefined)fs.closeSync(fd);}}

///|
fn run(values : Array[String]) -> Unit raise @mcap.McapError {
  if values.length() != 2 ||
    !["legacy", "direct", "managed"].contains(values[0]) {
    raise @mcap.io_error("usage: io legacy|direct|managed PATH")
  }
  let mode = values[0]
  let path = values[1]
  let file = if mode == "managed" {
    Some(@nodeio.open_managed_file(path))
  } else {
    None
  }
  let result = try {
    let base = @nodeio.open_file(path)
    let source = if mode == "legacy" {
      @mcap.new_read_source(base.size(), fn(offset, length) {
        let hex = legacy_read(path, offset.to_string(), length)
        if hex.has_prefix("!") {
          raise @mcap.io_error(hex)
        }
        @mcap.decode_hex(hex, max_bytes=length)
      })
    } else {
      match file {
        Some(file) => file.source()
        None => base
      }
    }
    let start = clock_ms()
    let mut calls = 0
    let mut checksum = 0UL
    for round = 0; round < 3; round = round + 1 {
      let mut offset = 0UL
      while offset < source.size() {
        let count = if source.size() - offset > 4096UL {
          4096
        } else {
          (source.size() - offset).to_int()
        }
        let bytes = source.read_exact(offset, count)
        checksum += bytes[0].to_uint64() + bytes[count - 1].to_uint64()
        offset += count.to_uint64()
        calls += 1
      }
    }
    let elapsed = clock_ms() - start
    let (physical, cached) = match file {
      Some(file) => (file.metrics().physical_reads, file.metrics().cached_bytes)
      None => (calls, 0)
    }
    let json : Json = {
      "mode": mode,
      "elapsed_ms": elapsed,
      "logical_reads": calls,
      "physical_reads": physical,
      "cached_bytes": cached,
      "checksum": checksum.to_string(),
      "sha256": @nodeio.source_sha256(base),
      "peak_rss_kib": rss_kib(),
    }
    Ok(json)
  } catch {
    error => Err(error)
  }
  if file is Some(file) {
    file.close()
  }
  match result {
    Ok(json) => println(json.stringify())
    Err(error) => raise error
  }
}

///|
fn main {
  run(args()) catch {
    @mcap.Invalid(reason) => report_error(reason)
  }
}
