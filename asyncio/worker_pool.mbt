///|
pub(all) struct WorkerOptions {
  workers : Int
  max_jobs : Int
  input_bytes : Int
  output_bytes : Int
  context_bytes : Int
  buffer_bytes : Int
  prefetch : Int
  min_chunk_bytes : Int
  min_payload_bytes : Int
  timeout_ms : Int
}

///|
pub fn worker_options() -> WorkerOptions {
  {
    workers: 2,
    max_jobs: 16,
    input_bytes: 16 * 1024 * 1024,
    output_bytes: 128 * 1024 * 1024,
    context_bytes: 1024 * 1024,
    buffer_bytes: 128 * 1024 * 1024,
    prefetch: 2,
    min_chunk_bytes: 65536,
    min_payload_bytes: 4096,
    timeout_ms: 30000,
  }
}

///|
pub struct WorkerPool {
  options : WorkerOptions
  submit : async (String, Int) -> String
  snapshot : () -> String
  shutdown : async () -> Unit
}

///|
pub fn WorkerPool::metrics(self : WorkerPool) -> Json raise @mcap.McapError {
  @json.parse((self.snapshot)()[:]) catch {
    _ => raise @mcap.io_error("invalid worker metrics")
  }
}

///|
pub async fn WorkerPool::close(self : WorkerPool) -> Unit {
  @async.protect_from_cancel(async fn() { (self.shutdown)() })
}

///|
async fn WorkerPool::call(
  self : WorkerPool,
  job : Json,
  maximum : Int,
) -> Bytes {
  let text = (self.submit)(job.stringify(), maximum)
  if text.has_prefix("!") {
    raise @mcap.io_error(text)
  }
  @mcap.decode_hex(text, max_bytes=maximum)
}

///|
pub async fn WorkerPool::expand_chunk(
  self : WorkerPool,
  body : Bytes,
  maximum : Int,
  limits? : @mcap.Limits = @mcap.default_limits(),
) -> Bytes {
  if maximum < 40 || maximum > self.options.output_bytes {
    raise @mcap.io_error("invalid parallel Chunk output bound")
  }
  let job : Json = {
    "op": "expand",
    "data": @mcap.encode_hex(body),
    "output_bytes": maximum,
    "limits": {
      "record": limits.max_record_bytes.min(self.options.input_bytes),
      "string": limits.max_string_bytes,
      "records": limits.max_records,
      "decompressed": limits.max_decompressed_bytes.min(maximum - 40),
    },
  }
  self.call(job, maximum)
}

///|
pub async fn WorkerPool::decode_message(
  self : WorkerPool,
  message : @mcap.Message,
  declarations : @mcap.Log,
  max_bytes? : Int = 1024 * 1024,
) -> @mcap.Message {
  if max_bytes < 1 || max_bytes > self.options.output_bytes {
    raise @mcap.io_error("invalid parallel decoded JSON bound")
  }
  let context = @mcap.write_log(declarations)
  if context.length() > self.options.context_bytes {
    raise @mcap.io_error("parallel declaration context exceeds budget")
  }
  let job : Json = {
    "op": "decode",
    "data": @mcap.encode_hex(message.data),
    "declarations": @mcap.encode_hex(context),
    "channel": message.channel_id,
    "output_bytes": max_bytes,
  }
  let data = self.call(job, max_bytes)
  { ..message, data, }
}
