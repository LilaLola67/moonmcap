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
