///|
test "chunk checkpoints reuse committed parts and match stable merge reference" {
  let log = cursor_fixture()
  let bytes = @moonmcap.write_indexed(
    log,
    compression=@moonmcap.Zstd,
    messages_per_chunk=17,
  )
  let source = @moonmcap.memory_source(bytes)
  let parts : Map[Int32, @buffer.Buffer] = Map([])
  let create = fn(id : Int32) -> @moonmcap.WriteSink raise @moonmcap.McapError {
    if parts.contains(id) {
      raise @moonmcap.io_error("committed part recreated")
    }
    let buffer = @buffer.Buffer::Buffer()
    parts[id] = buffer
    @moonmcap.new_write_sink(fn(bytes) { buffer.write_bytes(bytes[:]) })
  }
  let storage = @moonmcap.sort_storage(
    create,
    fn(id) { @moonmcap.memory_source(parts[id].to_bytes()) },
    fn(id) { parts.remove(id) },
  )
  let saved = [b""]
  let closed = [false]
  try
    @moonmcap.resumable_chunk_merge(
      [source, source],
      storage,
      memory_sort_storage(Map([]), closed),
      @moonmcap.new_write_sink(fn(_) { () }),
      index_test_fingerprint,
      fn(data) {
        saved[0] = data
        if parts.length() == 2 {
          raise @moonmcap.io_error("simulated stop after committed Chunk")
        }
      },
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("checkpoint interruption expected")
  }
  assert_true(closed[0])
  assert_eq(@moonmcap.checkpoint_run_ids(saved[0]), [0, 1])
  assert_true(
    @moonmcap.checkpoint_progress(saved[0])
    is {
      "engine": "indexed-chunk-ingest",
      "completed_inputs": 0,
      "next_chunk": 2,
      ..
    },
  )
  let out = @buffer.Buffer::Buffer()
  let report = @moonmcap.resumable_chunk_merge(
    [source, source],
    storage,
    memory_sort_storage(Map([]), [false]),
    @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }),
    index_test_fingerprint,
    fn(data) { saved[0] = data },
    checkpoint=saved[0],
  )
  let actual = @moonmcap.read_log(out.to_bytes())
  let wanted = @moonmcap.merge_logs([log, log])
  assert_eq(
    actual.messages.map(fn(m) { (m.log_time, m.sequence, m.data) }),
    wanted.messages.map(fn(m) { (m.log_time, m.sequence, m.data) }),
  )
  assert_eq(actual.attachments, wanted.attachments)
  assert_eq(actual.metadata, wanted.metadata)
  assert_true(report is { "source_complete": true, "input_files": 2, .. })
}

///|
test "chunk merge rejects missing CRC indexes and final data CRC before publication" {
  let log = cursor_fixture()
  let original = @moonmcap.write_indexed(log, messages_per_chunk=17)
  let records = @moonmcap.scan_file(original)
  let end = records.filter(fn(r) { r.opcode == 0x0f })[0].offset
  let chunk = records.filter(fn(r) { r.opcode == 0x06 })[0].offset
  for point in [end + 9, chunk + 9 + 24, original.length() - 12] {
    let bad = original.to_array()
    if point == chunk + 9 + 24 {
      for i = 0; i < 4; i = i + 1 {
        bad[point + i] = 0x00
      }
    } else {
      bad[point] = bad[point] ^ 0x40
    }
    let out = @buffer.Buffer::Buffer()
    let closed = [false]
    try
      @moonmcap.resumable_chunk_merge(
        [@moonmcap.memory_source(Bytes::from_array(bad))],
        memory_sort_storage(Map([]), [false]),
        memory_sort_storage(Map([]), closed),
        @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }),
        index_test_fingerprint,
        fn(_) { () },
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid source accepted")
    }
    assert_eq(out.length(), 0)
    assert_true(closed[0])
  }
}
