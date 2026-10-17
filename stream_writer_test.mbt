///|
test "stream writer emits complete indexed logs for all compression modes" {
  let log = cursor_fixture()
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let output = @buffer.Buffer::Buffer()
    let sink = @moonmcap.new_write_sink(fn(bytes) {
      output.write_bytes(bytes[:])
    })
    let writer = @moonmcap.new_stream_writer(sink, options={
      ..@moonmcap.stream_options(),
      compression,
      chunk_messages: 7,
      chunk_bytes: 1000,
    })
    for channel in log.channels {
      writer.add_channel(channel)
    }
    for message in log.messages {
      writer.append(message)
    }
    writer.add_attachment({
      log_time: 0UL,
      create_time: 0UL,
      name: "calibration",
      media_type: "raw",
      data: b"cal",
    })
    writer.add_metadata({ name: "run", entries: [("id", "stream")], })
    let done = writer.finish()
    let actual = @moonmcap.read_log(output.to_bytes())
    assert_eq(actual.messages, log.messages)
    assert_eq(actual.channels, log.channels)
    assert_eq(actual.attachments[0].data, b"cal")
    assert_eq(actual.metadata[0].name, "run")
    assert_eq(done.messages, 257UL)
    assert_eq(done.state, @moonmcap.WriterFinished)
    assert_eq(done.buffered_bytes, 0)
    assert_true(done.peak_buffered_bytes <= 1000)
    assert_eq(done.bytes_written, output.length().to_uint64())
    assert_eq(
      @moonmcap.open_reader(@moonmcap.memory_source(output.to_bytes()))
      .query(@moonmcap.all_query())
      .length(),
      257,
    )
  }
}

///|
test "stream flush thresholds include bytes counts spans and manual flush" {
  for mode in [0, 1, 2, 3] {
    let output = @buffer.Buffer::Buffer()
    let writer = @moonmcap.new_stream_writer(
      @moonmcap.new_write_sink(fn(b) { output.write_bytes(b[:]) }),
      options={
        ..@moonmcap.stream_options(),
        chunk_messages: if mode == 0 {
          2
        } else {
          100
        },
        chunk_bytes: if mode == 1 {
          64
        } else {
          1024
        },
        chunk_span: if mode == 2 {
          1UL
        } else {
          0UL
        },
      },
    )
    writer.add_channel({
      id: 1,
      schema_id: 0,
      topic: "/stream",
      message_encoding: "raw",
      metadata: [],
    })
    for i = 0; i < 5; i = i + 1 {
      writer.append({
        channel_id: 1,
        sequence: 0U,
        log_time: i.to_uint64(),
        publish_time: 0UL,
        data: b"x",
      })
      if mode == 3 {
        writer.flush()
      }
    }
    writer.flush()
    writer.flush()
    let done = writer.finish()
    assert_eq(done.chunks, if mode == 3 { 5U } else { 3U })
    assert_eq(@moonmcap.read_log(output.to_bytes()).messages.length(), 5)
  }
}

///|
test "stream invalid message is atomic and closed states reject writes" {
  let output = @buffer.Buffer::Buffer()
  let writer = @moonmcap.new_stream_writer(
    @moonmcap.new_write_sink(fn(b) { output.write_bytes(b[:]) }),
  )
  let before = writer.progress()
  try
    writer.append({
      channel_id: 1,
      sequence: 0U,
      log_time: 0UL,
      publish_time: 0UL,
      data: b"",
    })
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("unknown channel")
  }
  assert_eq(writer.progress(), before)
  ignore(writer.finish())
  try writer.finish() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("double finish")
  }
  try writer.flush() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("flush after finish")
  }
  assert_eq(writer.progress().state, @moonmcap.WriterFinished)
  assert_eq(@moonmcap.read_log(output.to_bytes()).messages.length(), 0)
}

///|
test "sink failure permanently poisons writer and cancellation leaves a prefix" {
  let output = @buffer.Buffer::Buffer()
  let fail_writes = [false]
  let writer = @moonmcap.new_stream_writer(
    @moonmcap.new_write_sink(fn(bytes) {
      if fail_writes[0] {
        raise @moonmcap.io_error("disk full")
      }
      output.write_bytes(bytes[:])
    }),
  )
  writer.add_channel({
    id: 1,
    schema_id: 0,
    topic: "/stream",
    message_encoding: "raw",
    metadata: [],
  })
  writer.append({
    channel_id: 1,
    sequence: 0U,
    log_time: 0UL,
    publish_time: 0UL,
    data: b"payload",
  })
  fail_writes[0] = true
  try writer.flush() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("sink failure")
  }
  assert_eq(writer.progress().state, @moonmcap.WriterFailed)
  fail_writes[0] = false
  try writer.finish() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("failed writer cannot finish")
  }
  let report = @moonmcap.recover_prefix(output.to_bytes())
  assert_true(!report.complete)
  let other = @moonmcap.new_stream_writer(
    @moonmcap.new_write_sink(fn(_) { () }),
  )
  other.cancel()
  other.cancel()
  assert_eq(other.progress().state, @moonmcap.WriterCancelled)
  try other.finish() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("cancelled writer")
  }
}

///|
test "stream enforces summary and chunk budgets without retaining full log" {
  let writer = @moonmcap.new_stream_writer(
    @moonmcap.new_write_sink(fn(_) { () }),
    options={ ..@moonmcap.stream_options(), summary_bytes: 1, },
  )
  let before = writer.progress()
  try
    writer.add_channel({
      id: 1,
      schema_id: 0,
      topic: "/a",
      message_encoding: "raw",
      metadata: [],
    })
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("summary budget")
  }
  assert_eq(writer.progress(), before)
  let bounded = @moonmcap.new_stream_writer(
    @moonmcap.new_write_sink(fn(_) { () }),
    options={ ..@moonmcap.stream_options(), chunk_bytes: 64, },
  )
  bounded.add_channel({
    id: 1,
    schema_id: 0,
    topic: "/a",
    message_encoding: "raw",
    metadata: [],
  })
  try
    bounded.append({
      channel_id: 1,
      sequence: 0U,
      log_time: 0UL,
      publish_time: 0UL,
      data: Bytes::make(34, b'x'),
    })
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("large message")
  }
  assert_eq(bounded.progress().messages, 0UL)
}
