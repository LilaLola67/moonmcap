///|
struct ArtifactMemory {
  mut checkpoint : Bytes?
  data : Map[Int32, Bytes]
  published : Map[Int32, Bool]
  mut saves : Int32
  mut fail_save : Int32
  mut fail_publish : Bool
}

///|
fn artifact_memory() -> ArtifactMemory {
  {
    checkpoint: None,
    data: Map([]),
    published: Map([]),
    saves: 0,
    fail_save: -1,
    fail_publish: false,
  }
}

///|
fn ArtifactMemory::storage(
  self : ArtifactMemory,
) -> @moonmcap.ArtifactBatchStorage {
  @moonmcap.artifact_batch_storage(
    fn() { self.checkpoint },
    fn(bytes) {
      self.checkpoint = Some(bytes)
      self.saves += 1
      if self.saves == self.fail_save {
        raise @moonmcap.io_error("simulated crash after durable save")
      }
    },
    fn(id) {
      if self.data.contains(id) {
        raise @moonmcap.io_error("artifact already exists")
      }
      self.data[id] = b""
      @moonmcap.new_write_sink(fn(bytes) {
        self.data[id] = self.data[id] + bytes
      })
    },
    fn(_) { () },
    fn(id) {
      guard self.data.get(id) is Some(bytes) else {
        raise @moonmcap.io_error("missing artifact")
      }
      let digest = @moonmcap.crc32(bytes).to_string()
      // Test-only content identity; the filesystem adapter uses actual SHA-256.
      let mut padded = digest
      while padded.length() < 64 {
        padded = "0" + padded
      }
      padded
    },
    fn(id) {
      self.published[id] = true
      if self.fail_publish {
        self.fail_publish = false
        raise @moonmcap.io_error("crash after publish")
      }
    },
    fn(id) { self.published.get(id).unwrap_or(false) },
    fn(id) { self.data.remove(id) |> ignore },
    fn() { self.data.keys().to_array() },
  )
}

///|
fn artifact_execute(
  id : Int32,
  sink : @moonmcap.WriteSink,
) -> Json raise @moonmcap.McapError {
  sink.write([id.to_byte()])
  { "id": id }
}

///|
test "artifact batches resume complete outputs and reject changed plans or content" {
  let memory = artifact_memory()
  let _ = @moonmcap.run_artifact_batch(
    "source+recipe+version",
    3,
    memory.storage(),
    artifact_execute,
    stop_after=1,
  )
  assert_eq(memory.data.length(), 1)
  let first = memory.data[0]
  let _ = @moonmcap.run_artifact_batch(
    "source+recipe+version",
    3,
    memory.storage(),
    artifact_execute,
  )
  assert_eq(memory.data[0], first)
  assert_eq(memory.published.length(), 3)
  let _ = @moonmcap.run_artifact_batch(
    "source+recipe+version",
    3,
    memory.storage(),
    fn(_, _) { raise @moonmcap.io_error("completed batch re-executed") },
  )
  try
    @moonmcap.run_artifact_batch(
      "different-source",
      3,
      memory.storage(),
      artifact_execute,
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("changed plan accepted")
  }
  memory.data[0] = b"tampered"
  try
    @moonmcap.run_artifact_batch(
      "source+recipe+version",
      3,
      memory.storage(),
      artifact_execute,
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("modified completed artifact accepted")
  }
}

///|
test "artifact batches recover prepared receipts on either side of publication" {
  for after_publish in [false, true] {
    let memory = artifact_memory()
    if after_publish {
      memory.fail_publish = true
    } else {
      memory.fail_save = 2
    }
    try
      @moonmcap.run_artifact_batch(
        "plan",
        1,
        memory.storage(),
        artifact_execute,
      )
    catch {
      _ => ()
    } noraise {
      _ => fail("fault injection did not interrupt")
    }
    assert_eq(memory.data.length(), 1)
    let _ = @moonmcap.run_artifact_batch("plan", 1, memory.storage(), fn(_, _) {
      raise @moonmcap.io_error("prepared artifact was re-executed")
    })
    assert_true(memory.published[0])
  }
}

///|
test "artifact batches restart incomplete writes and fail closed on orphan finals" {
  let memory = artifact_memory()
  try
    @moonmcap.run_artifact_batch("plan", 1, memory.storage(), fn(_, sink) {
      sink.write(b"partial")
      raise @moonmcap.io_error("interrupted writer")
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("writer fault not observed")
  }
  let _ = @moonmcap.run_artifact_batch(
    "plan",
    1,
    memory.storage(),
    artifact_execute,
  )
  assert_eq(memory.data[0], b"\x00")
  let orphan = artifact_memory()
  orphan.data[0] = b"orphan"
  orphan.published[0] = true
  try
    @moonmcap.run_artifact_batch("plan", 1, orphan.storage(), artifact_execute)
  catch {
    _ => ()
  } noraise {
    _ => fail("unreceipted final accepted")
  }
  assert_eq(orphan.data[0], b"orphan")
}

///|
test "artifact batches reject corrupt checkpoints and unexpected inventory" {
  let memory = artifact_memory()
  let _ = @moonmcap.run_artifact_batch(
    "plan",
    2,
    memory.storage(),
    artifact_execute,
    stop_after=0,
  )
  let bytes = memory.checkpoint.unwrap()
  memory.checkpoint = Some(bytes[0:bytes.length() - 1].to_owned() + b"x")
  try
    @moonmcap.run_artifact_batch("plan", 2, memory.storage(), artifact_execute)
  catch {
    _ => ()
  } noraise {
    _ => fail("corrupt checkpoint accepted")
  }
  memory.checkpoint = Some(bytes)
  memory.data[1] = b"future"
  try
    @moonmcap.run_artifact_batch("plan", 2, memory.storage(), artifact_execute)
  catch {
    _ => ()
  } noraise {
    _ => fail("future artifact accepted")
  }
  let cancelled = artifact_memory()
  try
    @moonmcap.run_artifact_batch(
      "plan",
      2,
      cancelled.storage(),
      artifact_execute,
      cancelled=fn() { true },
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("cancel ignored")
  }
  assert_eq(cancelled.checkpoint, None)
}
