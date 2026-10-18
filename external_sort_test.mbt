///|
fn memory_sort_storage(
  files : Map[Int, @buffer.Buffer],
  closed : Array[Bool],
) -> @moonmcap.SortStorage {
  @moonmcap.sort_storage(
    fn(id) {
      let out = @buffer.Buffer::Buffer()
      files[id] = out
      @moonmcap.new_write_sink(fn(bytes) { out.write_bytes(bytes[:]) })
    },
    fn(id) { @moonmcap.memory_source(files[id].to_bytes()) },
    fn(id) { files.remove(id) },
    finish=fn() { closed[0] = true },
  )
}

///|
test "external merge uses multiple bounded runs and preserves stable duplicates" {
  let log = cursor_fixture()
  let expected = @moonmcap.merge_logs([log, log])
  let sources = [
    @moonmcap.memory_source(
      @moonmcap.write_indexed(
        log,
        compression=@moonmcap.Zstd,
        messages_per_chunk=7,
      ),
    ),
    @moonmcap.memory_source(@moonmcap.write_log(log)),
  ]
  let output = @buffer.Buffer::Buffer()
  let files : Map[Int, @buffer.Buffer] = Map([])
  let closed = [false]
  let options = {
    ..@moonmcap.external_options(),
    run_bytes: 400,
    merge_bytes: 1000,
    fan_in: 2,
    output: { ..@moonmcap.stream_options(), compression: @moonmcap.Lz4, },
  }
  let report = @moonmcap.external_merge(
    sources,
    memory_sort_storage(files, closed),
    @moonmcap.new_write_sink(fn(bytes) { output.write_bytes(bytes[:]) }),
    options~,
  )
  let actual = @moonmcap.read_log(output.to_bytes())
  assert_eq(actual.messages, expected.messages)
  assert_eq(actual.channels, expected.channels)
  assert_true(report.runs_created > 10 && report.merge_passes > 1)
  assert_true(report.peak_run_bytes <= 400 && report.peak_merge_bytes <= 1000)
  assert_true(report.peak_spool_bytes <= options.spool_bytes)
  assert_eq(report.messages, 514UL)
  assert_true(files.is_empty() && closed[0])
}

///|
test "external merge remaps conflicting IDs and retains attachments and metadata" {
  let first = example_log()
  let second = {
    ..@moonmcap.copy_log(first),
    channels: first.channels.map(fn(c) { { ..c, topic: c.topic + "/other", } }),
  }
  first.attachments.push({
    name: "attachment",
    media_type: "raw",
    log_time: 1UL,
    create_time: 0UL,
    data: b"data",
  })
  first.metadata.push({ name: "run", entries: [("id", "first")], })
  let files : Map[Int, @buffer.Buffer] = Map([])
  let closed = [false]
  let output = @buffer.Buffer::Buffer()
  ignore(
    @moonmcap.external_merge(
      [
        @moonmcap.memory_source(@moonmcap.write_indexed(first)),
        @moonmcap.memory_source(@moonmcap.write_chunked(second)),
      ],
      memory_sort_storage(files, closed),
      @moonmcap.new_write_sink(fn(b) { output.write_bytes(b[:]) }),
    ),
  )
  let actual = @moonmcap.read_log(output.to_bytes())
  let expected = @moonmcap.merge_logs([first, second])
  assert_eq(actual.messages, expected.messages)
  assert_eq(actual.channels, expected.channels)
  assert_eq(actual.attachments, expected.attachments)
  assert_eq(actual.metadata, expected.metadata)
}

///|
test "external merge cleans temporary storage on budget failure and cancellation" {
  let bytes = @moonmcap.write_indexed(cursor_fixture())
  for mode in [0, 1, 2] {
    let files : Map[Int, @buffer.Buffer] = Map([])
    let closed = [false]
    let options = {
      ..@moonmcap.external_options(),
      run_bytes: if mode == 0 {
        43
      } else {
        400
      },
      spool_bytes: if mode == 1 {
        20UL
      } else {
        512000UL
      },
    }
    try
      @moonmcap.external_merge(
        [@moonmcap.memory_source(bytes)],
        memory_sort_storage(files, closed),
        @moonmcap.new_write_sink(fn(_) { () }),
        options~,
        cancelled=fn() { mode == 2 },
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("budget/cancellation failure was ignored")
    }
    assert_true(files.is_empty() && closed[0])
  }
}

///|
test "external merge rejects corruption in inputs and temporary runs" {
  let log = cursor_fixture()
  let valid = @moonmcap.write_indexed(log)
  for corrupt_run in [false, true] {
    let files : Map[Int, @buffer.Buffer] = Map([])
    let closed = [false]
    let storage = @moonmcap.sort_storage(
      fn(id) {
        let out = @buffer.Buffer::Buffer()
        files[id] = out
        @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) })
      },
      fn(id) {
        let bytes = files[id].to_bytes()
        @moonmcap.memory_source(
          if corrupt_run {
            Bytes::makei(bytes.length(), fn(i) {
              if i == 25 {
                bytes[i] ^ b'\x80'
              } else {
                bytes[i]
              }
            })
          } else {
            bytes
          },
        )
      },
      fn(id) { files.remove(id) },
      finish=fn() { closed[0] = true },
    )
    let input = if corrupt_run {
      valid
    } else {
      valid[0:valid.length() - 1].to_owned()
    }
    try
      @moonmcap.external_merge(
        [@moonmcap.memory_source(input)],
        storage,
        @moonmcap.new_write_sink(fn(_) { () }),
        options={ ..@moonmcap.external_options(), run_bytes: 400, },
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("corruption accepted")
    }
    assert_true(files.is_empty() && closed[0])
  }
}

///|
test "external cancellation after ingestion starts removes every run" {
  let bytes = @moonmcap.write_indexed(cursor_fixture(), messages_per_chunk=9)
  let files : Map[Int, @buffer.Buffer] = Map([])
  let closed = [false]
  let checks = [0]
  try
    @moonmcap.external_merge(
      [@moonmcap.memory_source(bytes)],
      memory_sort_storage(files, closed),
      @moonmcap.new_write_sink(fn(_) { () }),
      options={ ..@moonmcap.external_options(), run_bytes: 128, },
      cancelled=fn() {
        checks[0] += 1
        checks[0] > 50
      },
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("late cancellation ignored")
  }
  assert_true(checks[0] > 50 && closed[0] && files.is_empty())
}
