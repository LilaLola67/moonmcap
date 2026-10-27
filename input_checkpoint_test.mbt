///|
test "input checkpoint resumes without reparsing completed source chunks" {
  let log = cursor_fixture()
  let bytes = @moonmcap.write_indexed(
    log,
    compression=@moonmcap.Zstd,
    messages_per_chunk=5,
  )
  let offsets = @moonmcap.scan_file(bytes)
    .filter(fn(r) { r.opcode == 0x06 })
    .map(fn(r) { r.offset.to_uint64() })
  let blocked = [false]
  let first = @moonmcap.new_read_source(bytes.length().to_uint64(), fn(
    offset,
    length,
  ) {
    if blocked[0] &&
      offsets.any(fn(start) { offset == start || offset == start + 9UL }) {
      raise @moonmcap.io_error("completed input was parsed again")
    }
    bytes[offset.to_int():offset.to_int() + length].to_owned()
  })
  let sources = [first, @moonmcap.memory_source(bytes)]
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
        raise @moonmcap.io_error("stop after first input")
      },
      options~,
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("expected interruption")
  }
  guard @moonmcap.checkpoint_progress(saved[0]) is Object(progress) else {
    fail("progress")
  }
  assert_eq(progress["completed_inputs"], Json::number(1))
  assert_eq(progress["stage"], Json::string("ingest"))
  assert_true(@moonmcap.checkpoint_run_ids(saved[0]).contains(-2))
  assert_false(@moonmcap.checkpoint_run_ids(saved[0]).contains(-1))
  blocked[0] = true
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
  assert_eq(
    @moonmcap.read_log(out.to_bytes()).messages,
    @moonmcap.build_query_index(@moonmcap.merge_logs([log, log])).query(
      @moonmcap.all_query(),
    ),
  )
}

///|
test "version one merge checkpoints remain resumable and upgrade to version two" {
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
        if @moonmcap.checkpoint_progress(bytes) is Object(fields) &&
          fields["stage"] == Json::string("merge") {
          raise @moonmcap.io_error("stop")
        }
      },
      options~,
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("expected stop")
  }
  let body = b"MJOB1\r\n\x00" + saved[0][8:saved[0].length() - 12].to_owned()
  let legacy = body +
    @moonmcap.crc32(body).to_uint64().to_le_bytes()[0:4].to_owned()
  guard @moonmcap.checkpoint_progress(legacy) is Object(fields) else {
    fail("legacy progress")
  }
  assert_eq(fields["version"], Json::number(1))
  let owned = @moonmcap.checkpoint_run_ids(legacy)
  for id in files.keys().to_array() {
    if !owned.contains(id) {
      files.remove(id)
    }
  }
  let out = @buffer.Buffer::Buffer()
  ignore(
    @moonmcap.resumable_merge(
      sources,
      storage,
      @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }),
      checkpoint_test_hash,
      fn(bytes) { saved[0] = bytes },
      checkpoint=legacy,
      options~,
    ),
  )
  assert_eq(@moonmcap.read_log(out.to_bytes()).messages.length(), 257)
  guard @moonmcap.checkpoint_progress(saved[0]) is Object(upgraded) else {
    fail("upgrade")
  }
  assert_eq(upgraded["version"], Json::number(2))
}
