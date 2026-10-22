///|
async test "unified async datasets combine sidecars JSON filtering paging and seek" {
  let builder = @mcap.new_builder()
  let id = builder.register_channel("/json", "json")
  for i = 0; i < 20; i = i + 1 {
    builder.append(id, (19 - i).to_uint64(), @utf8.encode("{\"n\":\{i}}"[:]))
  }
  let bytes = builder.finish(compression=@mcap.Zstd, messages_per_chunk=3)
  let source = @asyncio.new_source(bytes.length().to_uint64(), async fn(
    offset,
    length,
  ) {
    @async.pause()
    bytes[offset.to_int():offset.to_int() + length].to_owned()
  })
  let digest = @asyncio.source_sha256(source)
  let index = @mcap.build_sidecar(@mcap.memory_source(bytes), fn(_) { digest })
  let sidecar = @asyncio.open_sidecar(
    source,
    index,
    @asyncio.AsyncVerifyContent(@asyncio.source_sha256),
    cache_bytes=0,
  )
  let ordinary = @asyncio.open_reader(source)
  let filter = @mcap.json_filter("{\"path\":\"/n\",\"op\":\">=\",\"value\":10}")
  let dataset = @asyncio.open_dataset([
    sidecar.query_source().filter_json(filter),
    ordinary.query_source().filter_json(filter),
  ])
  let expected = @mcap.open_dataset([
    @mcap.build_query_index(builder.snapshot())
    .query_source()
    .filter_json(filter),
    @mcap.build_query_index(builder.snapshot())
    .query_source()
    .filter_json(filter),
  ])
  for descending in [false, true] {
    let query = { ..@mcap.all_query(), descending, offset: 3, limit: 7, }
    let cursor = dataset.cursor(query)
    let actual = cursor.next_page(2) + cursor.next_page(5)
    assert_eq(actual, expected.cursor(query).next_page(7))
    cursor.seek(5UL)
    let adjusted = {
      ..query,
      offset: 0,
      start_time: if descending {
        0UL
      } else {
        5UL
      },
      end_time: if descending {
        5UL
      } else {
        18446744073709551615UL
      },
    }
    assert_eq(cursor.next_page(7), expected.cursor(adjusted).next_page(7))
    cursor.cancel()
    assert_eq(cursor.next(), None)
  }
  let output = @buffer.Buffer::Buffer()
  let count = dataset
    .cursor({ ..@mcap.all_query(), limit: 3, })
    .write_ndjson(
      @asyncio.new_sink(async fn(bytes) {
        @async.pause()
        output.write_bytes(bytes[:])
      }),
      content=filter,
    )
  assert_eq(count, 3)
  let tiny = sidecar.cursor(@mcap.all_query(), pending_bytes=1)
  try tiny.next() catch {
    _ => ()
  } noraise {
    _ => fail("sidecar span budget")
  }
}

///|
async test "async dataset cancellation rejects concurrency and unordered providers" {
  let builder = @mcap.new_builder()
  let id = builder.register_channel("/raw", "raw")
  builder.append(id, 2UL, b"a")
  builder.append(id, 1UL, b"b")
  let log = builder.snapshot()
  let declarations = { ..log, messages: [], }
  let provider = @asyncio.async_query_source(declarations, fn(_, _) {
    let index = [0]
    @asyncio.async_message_stream(
      async fn() {
        @async.pause()
        if index[0] >= 2 {
          None
        } else {
          let value = log.messages[index[0]]
          index[0] += 1
          Some(value)
        }
      },
      fn() { () },
    )
  })
  let cursor = @asyncio.open_dataset([provider]).cursor(@mcap.all_query())
  ignore(cursor.next())
  try cursor.next() catch {
    _ => ()
  } noraise {
    _ => fail("unordered provider accepted")
  }
  let cursor = @asyncio.open_dataset([provider]).cursor(@mcap.all_query())
  @async.with_task_group(async fn(group) {
    let task = group.spawn(async fn() { cursor.next() })
    @async.pause()
    try cursor.next() catch {
      _ => ()
    } noraise {
      _ => fail("concurrent read accepted")
    }
    cursor.cancel()
    assert_eq(task.wait(), None)
  })
}

///|
async test "timed playback injects clock waits backpressure and cancellation" {
  let builder = @mcap.new_builder()
  let id = builder.register_channel("/replay", "raw")
  for time in [100000000UL, 200000000UL, 300000000UL] {
    builder.append(id, time, b"x")
  }
  let replay = @mcap.replay(
    @mcap.build_query_index(builder.snapshot()).query_source(),
  )
  let clock = [0UL]
  let emitted = []
  @asyncio.playback(
    replay,
    fn() { clock[0] },
    async fn(delay) {
      @async.pause()
      clock[0] += delay
    },
    async fn(message) {
      emitted.push((message.log_time, clock[0]))
      @async.pause()
      if emitted.length() == 1 {
        replay.set_rate(2, 1, clock[0])
      }
    },
  )
  assert_eq(emitted, [
    (100000000UL, 0UL),
    (200000000UL, 50000000UL),
    (300000000UL, 100000000UL),
  ])
  let replay = @mcap.replay(
    @mcap.build_query_index(builder.snapshot()).query_source(),
  )
  let stopped = [false]
  @asyncio.playback(
    replay,
    fn() { clock[0] },
    async fn(_) { @async.pause() },
    async fn(_) { stopped[0] = true },
    cancelled=fn() { stopped[0] },
  )
  assert_eq(replay.progress(clock[0]).state, @mcap.ReplayCancelled)
}

///|
extern "js" fn binary_test_path() -> String =
  #| ()=>{const fs=process.getBuiltinModule('node:fs'),p=process.getBuiltinModule('node:path');return p.join(fs.mkdtempSync(p.join(process.getBuiltinModule('node:os').tmpdir(),'moonmcap-bytes-')),'data');}

///|
extern "js" fn binary_test_cleanup(path : String) -> Unit =
  #| path=>{const fs=process.getBuiltinModule('node:fs');fs.unlinkSync(path);fs.rmdirSync(process.getBuiltinModule('node:path').dirname(path));}

///|
async test "async binary file bridge preserves every byte and cancelled reads" {
  let path = binary_test_path()
  let b = @buffer.Buffer::Buffer()
  for i = 0; i < 8192; i = i + 1 {
    b.write_byte((i % 256).to_byte())
  }
  let bytes = b.to_bytes()
  let sink = @asyncio.create_sink(path)
  sink.write(bytes[0:256].to_owned())
  sink.write(bytes[256:].to_owned())
  let source = @asyncio.open_file(path)
  assert_eq(source.read_exact(0UL, 8192), bytes)
  assert_eq(source.read_exact(255UL, 1000), bytes[255:1255].to_owned())
  source.cancel()
  try source.read_exact(0UL, 1) catch {
    _ => ()
  } noraise {
    _ => fail("cancelled binary read")
  }
  binary_test_cleanup(path)
}

///|
async test "async file protocol suspends and preserves sorting across small pages" {
  let builder = @mcap.new_builder()
  let channel = builder.register_channel("/test", "raw")
  for time in [30UL, 10UL, 20UL, 20UL, 40UL] {
    builder.append(channel, time, b"payload")
  }
  let bytes = builder.finish(compression=@mcap.Zstd, messages_per_chunk=2)
  let calls = [0]
  let source = @asyncio.new_source(bytes.length().to_uint64(), async fn(
    offset,
    length,
  ) {
    @async.pause()
    calls[0] += 1
    let start = offset.to_int()
    bytes[start:start + length].to_owned()
  })
  let reader = @asyncio.open_reader(source)
  let cursor = reader.cursor(@mcap.all_query())
  let messages = []
  while true {
    let page = cursor.next_page(2)
    if page.is_empty() {
      break
    }
    for message in page {
      messages.push(message)
    }
  }
  assert_eq(
    messages,
    @mcap.build_query_index(builder.snapshot()).query(@mcap.all_query()),
  )
  assert_true(calls[0] > 5)
  cursor.seek(20UL)
  assert_eq(cursor.next_page(2).map(fn(m) { m.log_time }), [20UL, 20UL])
  cursor.cancel()
  assert_eq(cursor.next(), None)
}

///|
async test "async source short reads cancellation and errors are explicit" {
  let source = @asyncio.new_source(6UL, async fn(offset, length) {
    @async.pause()
    let start = offset.to_int()
    b"abcdef"[start:start + length.min(2)].to_owned()
  })
  assert_eq(source.read_exact(0UL, 6), b"abcdef")
  assert_eq(source.metrics().read_calls, 3)
  source.cancel()
  try source.read_exact(0UL, 1) catch {
    _ => ()
  } noraise {
    _ => fail("cancelled source")
  }
  let stalled = @asyncio.new_source(1UL, async fn(_, _) { b"" })
  try stalled.read_exact(0UL, 1) catch {
    _ => ()
  } noraise {
    _ => fail("zero progress")
  }
}

///|
async test "async writer applies sequential backpressure and writes complete indexes" {
  let output = @buffer.Buffer::Buffer()
  let active = [0, 0]
  let sink = @asyncio.new_sink(async fn(bytes) {
    active[0] += 1
    active[1] = active[1].max(active[0])
    @async.pause()
    output.write_bytes(bytes[:])
    active[0] -= 1
  })
  let writer = @asyncio.new_writer(
    sink,
    options={
      ..@mcap.stream_options(),
      chunk_bytes: 128,
      chunk_messages: 3,
      compression: @mcap.Lz4,
    },
    queue_bytes=8192,
  )
  writer.add_channel({
    id: 1,
    schema_id: 0,
    topic: "/async",
    message_encoding: "raw",
    metadata: [],
  })
  for i = 0; i < 40; i = i + 1 {
    writer.append({
      channel_id: 1,
      sequence: i.reinterpret_as_uint(),
      log_time: i.to_uint64(),
      publish_time: i.to_uint64(),
      data: b"event",
    })
  }
  let progress = writer.finish()
  assert_true(progress.state is @asyncio.AsyncFinished)
  assert_eq(progress.queued_bytes, 0)
  assert_true(progress.peak_queued_bytes <= 8192)
  assert_eq(progress.bytes_written, output.length().to_uint64())
  assert_eq(active[1], 1)
  assert_eq(@mcap.read_log(output.to_bytes()).messages.length(), 40)
}

///|
async test "async output failure and explicit cancellation are terminal" {
  let fail_writes = [false]
  let sink = @asyncio.new_sink(async fn(_) {
    @async.pause()
    if fail_writes[0] {
      raise @mcap.io_error("disk full")
    }
  })
  let writer = @asyncio.new_writer(sink)
  fail_writes[0] = true
  try
    writer.add_channel({
      id: 1,
      schema_id: 0,
      topic: "/test",
      message_encoding: "raw",
      metadata: [],
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("output failure")
  }
  assert_true(writer.progress().state is @asyncio.AsyncFailed)
  try writer.finish() catch {
    _ => ()
  } noraise {
    _ => fail("failed writer reopened")
  }
  let other = @asyncio.new_writer(
    @asyncio.new_sink(async fn(_) { @async.pause() }),
  )
  other.cancel()
  assert_true(other.progress().state is @asyncio.AsyncCancelled)
  try other.finish() catch {
    _ => ()
  } noraise {
    _ => fail("cancelled writer reopened")
  }
}

///|
async test "rotation produces independent complete files with all declarations" {
  let template = @mcap.empty_log()
  template.channels.push({
    id: 1,
    schema_id: 0,
    topic: "/segments",
    message_encoding: "raw",
    metadata: [],
  })
  let outputs : Array[@buffer.Buffer] = []
  let writer = @asyncio.rotating_writer(
    template,
    async fn(index) {
      @async.pause()
      assert_eq(index, outputs.length())
      let out = @buffer.Buffer::Buffer()
      outputs.push(out)
      @asyncio.new_sink(async fn(bytes) {
        @async.pause()
        out.write_bytes(bytes[:])
      })
    },
    segment_messages=3,
  )
  for i = 0; i < 10; i = i + 1 {
    writer.append({
      channel_id: 1,
      sequence: i.reinterpret_as_uint(),
      log_time: i.to_uint64(),
      publish_time: i.to_uint64(),
      data: b"data",
    })
  }
  let segments = writer.finish()
  assert_eq(segments.map(fn(s) { s.messages }), [3UL, 3UL, 3UL, 1UL])
  let actual = []
  for out in outputs {
    for message in @mcap.read_log(out.to_bytes()).messages {
      actual.push(message.sequence)
    }
  }
  assert_eq(actual, Array::makei(10, fn(i) { i.reinterpret_as_uint() }))
}

///|
async test "rotation time and byte targets preserve complete message order" {
  for mode in [0, 1] {
    let template = @mcap.empty_log()
    template.channels.push({
      id: 1,
      schema_id: 0,
      topic: "/rotate",
      message_encoding: "raw",
      metadata: [],
    })
    let outputs : Array[@buffer.Buffer] = []
    let writer = @asyncio.rotating_writer(
      template,
      async fn(_) {
        let out = @buffer.Buffer::Buffer()
        outputs.push(out)
        @asyncio.new_sink(async fn(bytes) {
          @async.pause()
          out.write_bytes(bytes[:])
        })
      },
      segment_messages=100,
      segment_span=if mode == 0 { 1UL } else { 0UL },
      segment_bytes_target=if mode == 1 { 1UL } else { 0UL },
    )
    for i = 0; i < 5; i = i + 1 {
      writer.append({
        channel_id: 1,
        sequence: i.reinterpret_as_uint(),
        log_time: i.to_uint64() * 10UL,
        publish_time: 0UL,
        data: b"x",
      })
    }
    assert_eq(writer.finish().length(), 5)
    for i = 0; i < outputs.length(); i = i + 1 {
      assert_eq(
        @mcap.read_log(outputs[i].to_bytes()).messages[0].sequence,
        i.reinterpret_as_uint(),
      )
    }
  }
}

///|
async test "one async sink cannot be claimed by a second writer" {
  let sink = @asyncio.new_sink(async fn(_) { @async.pause() })
  let writer = @asyncio.new_writer(sink)
  try @asyncio.new_writer(sink) catch {
    _ => ()
  } noraise {
    _ => fail("sink double ownership")
  }
  writer.cancel()
}
