///|
async test "writer constructor failure aborts its claimed sink exactly once" {
  let aborted = [0]
  let sink = @asyncio.new_sink(
    _ => raise @mcap.io_error("header write failed"),
    abort=() => aborted[0] += 1,
  )
  try @asyncio.new_writer(sink) catch {
    _ => ()
  } noraise {
    _ => fail("failed sink accepted")
  }
  assert_eq(aborted[0], 1)
  sink.cancel()
  assert_eq(aborted[0], 1)
}

///|
async test "invalid writer budgets do not claim or abort caller sink" {
  let aborted = [0]
  let sink = @asyncio.new_sink(_ => (), abort=() => aborted[0] += 1)
  try @asyncio.new_writer(sink, queue_bytes=16) catch {
    _ => ()
  } noraise {
    _ => fail("invalid queue budget accepted")
  }
  assert_eq(aborted[0], 0)
  let writer = @asyncio.new_writer(sink)
  assert_true(writer.finish().state is @asyncio.AsyncFinished)
  assert_eq(aborted[0], 0)
}

///|
async test "task cancellation during sink backpressure releases writer ownership" {
  let blocked = [false]
  let entered = [false]
  let aborted = [0]
  let sink = @asyncio.new_sink(
    async fn(bytes) {
      ignore(bytes)
      if blocked[0] {
        entered[0] = true
        @async.sleep(10000)
      }
    },
    abort=() => aborted[0] += 1,
  )
  let writer = @asyncio.new_writer(sink)
  blocked[0] = true
  @async.with_task_group(async fn(group) {
    let task = group.spawn(
      async fn() {
        writer.add_channel({
          id: 1,
          schema_id: 0,
          topic: "/n",
          message_encoding: "json",
          metadata: [],
        })
      },
      allow_failure=true,
    )
    while !entered[0] {
      @async.pause()
    }
    task.cancel()
    task.wait() catch {
      _ => ()
    }
  })
  assert_true(writer.progress().state is @asyncio.AsyncCancelled)
  assert_eq(writer.progress().queued_bytes, 0)
  assert_eq(aborted[0], 1)
  writer.cancel()
  sink.cancel()
  assert_eq(aborted[0], 1)
  try writer.finish() catch {
    _ => ()
  } noraise {
    _ => fail("cancelled writer reopened")
  }
}

///|
async test "ordinary sink errors remain failures and abort exactly once" {
  let failed = [false]
  let aborted = [0]
  let sink = @asyncio.new_sink(
    _ => if failed[0] { raise @mcap.io_error("write failed") },
    abort=() => aborted[0] += 1,
  )
  let writer = @asyncio.new_writer(sink)
  failed[0] = true
  try
    writer.add_channel({
      id: 1,
      schema_id: 0,
      topic: "/n",
      message_encoding: "json",
      metadata: [],
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("write failure accepted")
  }
  assert_true(writer.progress().state is @asyncio.AsyncFailed)
  assert_eq(writer.progress().queued_bytes, 0)
  assert_eq(aborted[0], 1)
  writer.cancel()
  sink.cancel()
  assert_eq(aborted[0], 1)
}

///|
async test "dataset task cancellation closes owned stream once with cancelled state" {
  let entered = [false]
  let stopped = [0]
  let template = @mcap.new_builder().snapshot()
  let source = @asyncio.async_query_source(template, (_, _) => {
    @asyncio.async_message_stream(
      async fn() {
        entered[0] = true
        @async.sleep(10000)
        None
      },
      () => stopped[0] += 1,
    )
  })
  let cursor = @asyncio.open_dataset([source]).cursor(@mcap.all_query())
  @async.with_task_group(async fn(group) {
    let task = group.spawn(async fn() { cursor.next() }, allow_failure=true)
    while !entered[0] {
      @async.pause()
    }
    task.cancel()
    ignore(task.wait() catch { _ => None })
  })
  assert_eq(cursor.progress().state, @mcap.FileCancelled)
  assert_eq(stopped[0], 1)
  cursor.cancel()
  cursor.cancel()
  assert_eq(stopped[0], 1)
  assert_eq(cursor.next(), None)
}

///|
async test "ordinary provider errors close stream once with failed state" {
  let stopped = [0]
  let source = @asyncio.async_query_source(@mcap.new_builder().snapshot(), (
    _,
    _,
  ) => {
    @asyncio.async_message_stream(
      () => raise @mcap.io_error("provider failure"),
      () => stopped[0] += 1,
    )
  })
  let cursor = @asyncio.open_dataset([source]).cursor(@mcap.all_query())
  try cursor.next() catch {
    _ => ()
  } noraise {
    _ => fail("provider error ignored")
  }
  assert_eq(cursor.progress().state, @mcap.FileFailed)
  assert_eq(stopped[0], 1)
  cursor.cancel()
  assert_eq(cursor.progress().state, @mcap.FileFailed)
  assert_eq(stopped[0], 1)
}

///|
async test "cursor cancellation preserves a caller-owned source shared by another cursor" {
  let builder = @mcap.new_builder()
  let channel = builder.register_channel("/n", "json")
  builder.append(channel, 1UL, b"{}")
  let bytes = builder.finish()
  let aborted = [0]
  let source = @asyncio.new_source(
    bytes.length().to_uint64(),
    (offset, length) => {
      bytes[offset.to_int():offset.to_int() + length].to_owned()
    },
    abort=() => aborted[0] += 1,
  )
  let reader = @asyncio.open_reader(source)
  let dataset = @asyncio.open_dataset([reader.query_source()])
  let first = dataset.cursor(@mcap.all_query())
  let second = dataset.cursor(@mcap.all_query())
  first.cancel()
  assert_true(second.next() is Some(row) && row.message.log_time == 1UL)
  assert_eq(aborted[0], 0)
  second.cancel()
  assert_eq(aborted[0], 0)
  source.cancel()
  source.cancel()
  assert_eq(aborted[0], 1)
}
