///|
// Selected after measuring uncovered validation paths, not for a coverage quota.
test "reader rejects negative budgets before performing any source read" {
  let reads = [0]
  let source = @moonmcap.new_read_source(100UL, fn(_, _) {
    reads[0] += 1
    raise @moonmcap.io_error("untrusted read must not start")
  })
  for options in [(-1, 8, 8), (8, -1, 8), (8, 8, -1)] {
    try
      @moonmcap.open_reader(
        source,
        cache_bytes=options.0,
        summary_bytes=options.1,
        fallback_bytes=options.2,
      )
    catch {
      @moonmcap.Invalid(reason) => assert_true(reason.contains("budgets"))
    } noraise {
      _ => fail("negative budget accepted")
    }
  }
  assert_eq(reads[0], 0)
}

///|
test "artifact checkpoints reject valid-CRC illegal pending state without side effects" {
  let memory = artifact_memory()
  ignore(
    @moonmcap.run_artifact_batch(
      "risk",
      2,
      memory.storage(),
      artifact_execute,
      stop_after=0,
    ),
  )
  let original = memory.checkpoint.unwrap()
  let body = original[0:original.length() - 4].to_owned().to_array()
  // 8-byte magic + 4-byte text length + four plan bytes + total + completed.
  body[24] = 2
  let out = @buffer.Buffer::Buffer()
  let invalid = Bytes::from_array(body)
  out.write_bytes(invalid[:])
  out.write_uint_le(@moonmcap.crc32(invalid))
  memory.checkpoint = Some(out.to_bytes())
  let before = memory.checkpoint
  let saves = memory.saves
  try
    @moonmcap.run_artifact_batch("risk", 2, memory.storage(), artifact_execute)
  catch {
    @moonmcap.Invalid(reason) => assert_true(reason.contains("pending"))
  } noraise {
    _ => fail("illegal pending state accepted")
  }
  assert_eq(memory.checkpoint, before)
  assert_eq(memory.saves, saves)
  assert_true(memory.data.is_empty() && memory.published.is_empty())
}

///|
test "short artifact snapshot is rejected before output creation" {
  let memory = artifact_memory()
  memory.checkpoint = Some(b"ABAT1")
  try
    @moonmcap.run_artifact_batch("risk", 1, memory.storage(), artifact_execute)
  catch {
    @moonmcap.Invalid(reason) => assert_true(reason.contains("size"))
  } noraise {
    _ => fail("truncated checkpoint accepted")
  }
  assert_eq(memory.saves, 0)
  assert_true(memory.data.is_empty())
}
