///|
/// Storage owns exclusive access, durable saves, staged files and publication.
/// `publish` must be idempotent and must never replace an unrelated output.
pub struct ArtifactBatchStorage {
  load : () -> Bytes? raise McapError
  save : (Bytes) -> Unit raise McapError
  create : (Int) -> WriteSink raise McapError
  seal : (Int) -> Unit raise McapError
  digest : (Int) -> String raise McapError
  publish : (Int) -> Unit raise McapError
  published : (Int) -> Bool raise McapError
  discard : (Int) -> Unit raise McapError
  inventory : () -> Array[Int] raise McapError
}

///|
pub fn artifact_batch_storage(
  load : () -> Bytes? raise McapError,
  save : (Bytes) -> Unit raise McapError,
  create : (Int) -> WriteSink raise McapError,
  seal : (Int) -> Unit raise McapError,
  digest : (Int) -> String raise McapError,
  publish : (Int) -> Unit raise McapError,
  published : (Int) -> Bool raise McapError,
  discard : (Int) -> Unit raise McapError,
  inventory : () -> Array[Int] raise McapError,
) -> ArtifactBatchStorage {
  { load, save, create, seal, digest, publish, published, discard, inventory, }
}

///|
struct ArtifactReceipt {
  digest : String
  report : Json
}

///|
fn artifact_digest_valid(digest : String) -> Bool {
  digest.length() == 64 &&
  digest.iter().all(fn(c) { (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') })
}

///|
fn artifact_checkpoint(
  plan : String,
  total : Int,
  completed : Array[ArtifactReceipt],
  pending : ArtifactReceipt?,
) -> Bytes raise McapError {
  let b = @buffer.Buffer::Buffer()
  b.write_bytes(b"ABAT1\r\n\x00"[:])
  checkpoint_text(b, plan)
  b.write_uint_le(total.to_uint())
  b.write_uint_le(completed.length().to_uint())
  b.write_uint_le(if pending is Some(_) { 1U } else { 0U })
  let receipts = match pending {
    Some(r) => completed + [r]
    None => completed
  }
  for r in receipts {
    if !artifact_digest_valid(r.digest) {
      raise Invalid("invalid artifact SHA-256")
    }
    checkpoint_text(b, r.digest)
    checkpoint_text(b, r.report.stringify())
  }
  let body = b.to_bytes()
  if body.length() > 8 * 1024 * 1024 - 4 {
    raise Invalid("artifact checkpoint exceeds 8 MiB")
  }
  body + crc32(body).mcap_le_bytes()
}

///|
fn restore_artifacts(
  bytes : Bytes,
  plan : String,
  total : Int,
) -> (Array[ArtifactReceipt], ArtifactReceipt?) raise McapError {
  if bytes.length() < 24 || bytes.length() > 8 * 1024 * 1024 {
    raise Invalid("invalid artifact checkpoint size")
  }
  let body = bytes[0:bytes.length() - 4].to_owned()
  if cursor(bytes[bytes.length() - 4:].to_owned(), default_limits()).u32() !=
    crc32(body) {
    raise Invalid("artifact checkpoint checksum mismatch")
  }
  let c = cursor(body, { ..default_limits(), max_string_bytes: 1024 * 1024, })
  if c.take(8) != b"ABAT1\r\n\x00" ||
    c.text() != plan ||
    c.u32() != total.to_uint() {
    raise Invalid("artifact plan, source or version changed")
  }
  let count = bounded_length(c.u32().to_uint64(), total, "completed artifacts")
  let pending = c.u32()
  if pending > 1U || (pending == 1U && count == total) {
    raise Invalid("invalid artifact pending state")
  }
  let completed : Array[ArtifactReceipt] = []
  let mut prepared : ArtifactReceipt? = None
  for i = 0; i < count + pending.to_int(); i = i + 1 {
    let digest = c.text()
    if !artifact_digest_valid(digest) {
      raise Invalid("invalid artifact digest")
    }
    let report = parse_json_document(@utf8.encode(c.text()[:]), 1024 * 1024, 64)
    let receipt = { digest, report, }
    if i < count {
      completed.push(receipt)
    } else {
      prepared = Some(receipt)
    }
  }
  c.finish()
  (completed, prepared)
}

///|
/// A checkpoint is committed before publishing each completed artifact. An
/// interrupted artifact is restarted; previously published artifacts are hashed
/// before reuse. The plan must identify input content, processing rules and code.
pub fn run_artifact_batch(
  plan : String,
  total : Int,
  storage : ArtifactBatchStorage,
  execute : (Int, WriteSink) -> Json raise McapError,
  stop_after? : Int = 4096,
  cancelled? : () -> Bool = fn() { false },
) -> Json raise McapError {
  if plan == "" ||
    @utf8.encode(plan[:]).length() > 1024 * 1024 ||
    total < 0 ||
    total > 4096 ||
    stop_after < 0 {
    raise Invalid("invalid artifact batch plan or budget")
  }
  if cancelled() {
    raise Invalid("artifact batch cancelled")
  }
  let (completed, pending) = match (storage.load)() {
    Some(bytes) => restore_artifacts(bytes, plan, total)
    None => ([], None)
  }
  let ids = (storage.inventory)()
  let seen : Map[Int, Bool] = Map([])
  for id in ids {
    if id < 0 || id >= total || id > completed.length() || seen.contains(id) {
      raise Invalid("unexpected artifact inventory")
    }
    seen[id] = true
  }
  // Validate everything already committed before altering any file.
  for i = 0; i < completed.length(); i = i + 1 {
    if !(storage.published)(i) || (storage.digest)(i) != completed[i].digest {
      raise Invalid("completed artifact missing or modified")
    }
  }
  let reused = completed.length()
  if pending is Some(receipt) {
    let id = completed.length()
    if (storage.digest)(id) != receipt.digest {
      raise Invalid("prepared artifact missing or modified")
    }
    (storage.publish)(id)
    completed.push(receipt)
    (storage.save)(artifact_checkpoint(plan, total, completed, None))
  } else if completed.length() < total && ids.contains(completed.length()) {
    // A final output without a durable receipt is never silently trusted.
    if (storage.published)(completed.length()) {
      raise Invalid("unreceipted published artifact")
    }
    (storage.discard)(completed.length())
  }
  (storage.save)(artifact_checkpoint(plan, total, completed, None))
  let mut executed = 0
  while completed.length() < total && executed < stop_after {
    if cancelled() {
      raise Invalid("artifact batch cancelled")
    }
    let id = completed.length()
    let sink = (storage.create)(id)
    let report = execute(id, sink)
    if @utf8.encode(report.stringify()[:]).length() > 1024 * 1024 {
      raise Invalid("artifact report exceeds 1 MiB")
    }
    (storage.seal)(id)
    let digest = (storage.digest)(id)
    let receipt = { digest, report, }
    // Failure after this save recovers publication without re-running execute.
    (storage.save)(artifact_checkpoint(plan, total, completed, Some(receipt)))
    (storage.publish)(id)
    completed.push(receipt)
    (storage.save)(artifact_checkpoint(plan, total, completed, None))
    executed += 1
  }
  {
    "format": "ABAT1",
    "total": total,
    "completed": completed.length(),
    "complete": completed.length() == total,
    "reused": reused,
    "executed": executed,
    "outputs": completed.mapi(fn(i, receipt) {
      (
        { "ordinal": i, "sha256": receipt.digest, "report": receipt.report } :
        Json)
    }),
  }
}
