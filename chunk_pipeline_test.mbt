///|
test "combined chunk checkpoints resume committed final merge groups" {
  let log = cursor_fixture()
  let source = @moonmcap.memory_source(
    @moonmcap.write_indexed(
      log,
      compression=@moonmcap.Zstd,
      messages_per_chunk=17,
    ),
  )
  let files : Map[Int, @buffer.Buffer] = Map([])
  let created : Map[Int, Int] = Map([])
  let storage = @moonmcap.sort_storage(
    fn(id) {
      if files.contains(id) {
        raise @moonmcap.io_error("committed pipeline file recreated")
      }
      created[id] = created.get_or_default(id, 0) + 1
      let buffer = @buffer.Buffer::Buffer()
      files[id] = buffer
      @moonmcap.new_write_sink(fn(bytes) { buffer.write_bytes(bytes[:]) })
    },
    fn(id) { @moonmcap.memory_source(files[id].to_bytes()) },
    fn(id) { files.remove(id) },
  )
  let saved = [b""]
  let options = {
    ..@moonmcap.external_options(),
    run_bytes: 512,
    merge_bytes: 8192,
    fan_in: 2,
  }
  try
    @moonmcap.resumable_chunk_pipeline(
      [source, source],
      storage,
      @moonmcap.new_write_sink(fn(_) { () }),
      index_test_fingerprint,
      fn(bytes) {
        saved[0] = bytes
        if @moonmcap.checkpoint_progress(bytes)
          is { "final_sort": { "merge_groups": 1, .. }, .. } {
          raise @moonmcap.io_error("stop after final merge group")
        }
      },
      options~,
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("expected final merge interruption")
  }
  assert_true(
    @moonmcap.checkpoint_progress(saved[0])
    is {
      "engine": "indexed-chunk-pipeline",
      "completed_inputs": 2,
      "final_sort": { "merge_groups": 1, .. },
      ..
    },
  )
  let ids = @moonmcap.checkpoint_run_ids(saved[0])
  for id in files.keys().to_array() {
    if !ids.contains(id) {
      files.remove(id)
    }
  }
  let before = created.copy()
  let output = @buffer.Buffer::Buffer()
  ignore(
    @moonmcap.resumable_chunk_pipeline(
      [source, source],
      storage,
      @moonmcap.new_write_sink(fn(data) { output.write_bytes(data[:]) }),
      index_test_fingerprint,
      fn(bytes) { saved[0] = bytes },
      options~,
      checkpoint=saved[0],
    ),
  )
  for id in ids {
    assert_eq(created[id], before[id])
  }
  let result = @moonmcap.read_log(output.to_bytes())
  let expected = @moonmcap.merge_logs([log, log])
  assert_eq(
    result.messages.map(fn(m) { (m.log_time, m.sequence, m.data) }),
    expected.messages.map(fn(m) { (m.log_time, m.sequence, m.data) }),
  )
  assert_eq(result.attachments, expected.attachments)
  assert_eq(result.metadata, expected.metadata)
}
