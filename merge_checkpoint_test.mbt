///|
fn checkpoint_test_hash(
  source : @moonmcap.ReadSource,
) -> String raise @moonmcap.McapError {
  @moonmcap.encode_hex(
    @moonmcap.crc32(source.read_exact(0UL, source.size().to_int())).to_le_bytes(),
  )
}

///|
test "resumable merge reuses completed runs after durable-save interruption" {
  let bytes = @moonmcap.write_indexed(cursor_fixture(), messages_per_chunk=7)
  let sources = [@moonmcap.memory_source(bytes), @moonmcap.memory_source(bytes)]
  let files : Map[Int, @buffer.Buffer] = Map([])
  let storage = memory_sort_storage(files, [false])
  let saved = [b""]
  let saves = [0]
  let options = { ..@moonmcap.external_options(), run_bytes: 1024, fan_in: 2, }
  try
    @moonmcap.resumable_merge(
      sources,
      storage,
      @moonmcap.new_write_sink(fn(_) { () }),
      checkpoint_test_hash,
      fn(bytes) {
        saved[0] = bytes
        saves[0] += 1
        if saves[0] == 3 {
          raise @moonmcap.io_error("simulated interruption")
        }
      },
      options~,
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("interruption expected")
  }
  assert_eq(saves[0], 3)
  let ids = @moonmcap.checkpoint_run_ids(saved[0])
  for id in files.keys().to_array() {
    if !ids.contains(id) {
      files.remove(id)
    }
  }
  let out = @buffer.Buffer::Buffer()
  let report = @moonmcap.resumable_merge(
    sources,
    storage,
    @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }),
    checkpoint_test_hash,
    fn(bytes) { saved[0] = bytes },
    checkpoint=saved[0],
    options~,
  )
  assert_eq(report.messages, 514UL)
  let actual = @moonmcap.read_log(out.to_bytes())
  let expected = @moonmcap.build_query_index(
    @moonmcap.merge_logs([cursor_fixture(), cursor_fixture()]),
  ).query(@moonmcap.all_query())
  assert_eq(actual.messages, expected)
  assert_true(files.contains(-1))
  assert_true(@moonmcap.checkpoint_run_ids(saved[0]).length() <= 3)
}

///|
test "resumable merge rejects changed inputs options checkpoint and run contents" {
  let bytes = @moonmcap.write_indexed(cursor_fixture())
  let sources = [@moonmcap.memory_source(bytes)]
  let files : Map[Int, @buffer.Buffer] = Map([])
  let storage = memory_sort_storage(files, [false])
  let saved = [b""]
  let options = { ..@moonmcap.external_options(), run_bytes: 1024, fan_in: 2, }
  try
    @moonmcap.resumable_merge(
      sources,
      storage,
      @moonmcap.new_write_sink(fn(_) { () }),
      checkpoint_test_hash,
      fn(bytes) {
        saved[0] = bytes
        raise @moonmcap.io_error("stop")
      },
      options~,
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("stop expected")
  }
  for mode in [0, 1, 2, 3] {
    let checkpoint = if mode == 0 { saved[0] + b"x" } else { saved[0] }
    let changed = if mode == 1 {
      [@moonmcap.memory_source(@moonmcap.write_indexed(example_log()))]
    } else {
      sources
    }
    let settings = if mode == 2 { { ..options, fan_in: 3, } } else { options }
    if mode == 3 {
      files[0].write_byte(0)
    }
    let writes = [0]
    try
      @moonmcap.resumable_merge(
        changed,
        storage,
        @moonmcap.new_write_sink(fn(_) { writes[0] += 1 }),
        checkpoint_test_hash,
        fn(_) { writes[0] += 1 },
        checkpoint~,
        options=settings,
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid checkpoint accepted")
    }
    assert_eq(writes[0], 0)
  }
}
