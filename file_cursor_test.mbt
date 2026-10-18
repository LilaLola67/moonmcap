///|
fn drain_file(
  cursor : @moonmcap.FileCursor,
  size : Int,
) -> Array[@moonmcap.Message] raise @moonmcap.McapError {
  let result = []
  while true {
    let page = cursor.next_page(size)
    if page.is_empty() {
      break
    }
    for message in page {
      result.push(message)
    }
  }
  result
}

///|
test "file cursor matches full sorting with overlapping chunks and stable ties" {
  let log = cursor_fixture()
  let reference = @moonmcap.build_query_index(log)
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let bytes = @moonmcap.write_indexed(log, compression~, messages_per_chunk=7)
    for descending in [false, true] {
      for topics in [[], ["/shared", "/shared"], ["/missing"]] {
        let query = {
          ..@moonmcap.all_query(),
          start_time: 3UL,
          end_time: 19UL,
          topics,
          descending,
          offset: 3,
          limit: 71,
        }
        let reader = @moonmcap.open_reader(
          @moonmcap.memory_source(bytes),
          cache_bytes=0,
        )
        let cursor = reader.cursor(query)
        assert_eq(drain_file(cursor, 4), reference.query(query))
        assert_eq(cursor.progress().state, @moonmcap.FileExhausted)
        assert_eq(cursor.progress().pending_bytes, 0)
      }
    }
  }
}

///|
test "file cursor first page and cancellation do not read all disjoint chunks" {
  let recorder = @moonmcap.new_builder()
  let channel = recorder.register_channel("/time", "raw")
  for i = 0; i < 100; i = i + 1 {
    recorder.append(channel, i.to_uint64(), b"payload")
  }
  let bytes = recorder.finish(messages_per_chunk=5)
  let reader = @moonmcap.open_reader(@moonmcap.memory_source(bytes))
  let cursor = reader.cursor(@moonmcap.all_query())
  let before = reader.diagnostics().bytes_read
  assert_eq(cursor.next_page(2).length(), 2)
  assert_eq(cursor.progress().loaded_chunks, 1)
  assert_true(reader.diagnostics().bytes_read > before)
  cursor.cancel()
  let stopped = reader.diagnostics().bytes_read
  assert_eq(cursor.next(), None)
  assert_eq(reader.diagnostics().bytes_read, stopped)
  try cursor.seek(0UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("cancelled seek")
  }
}

///|
test "file cursor seek resets offset and state with inclusive boundaries" {
  let log = cursor_fixture()
  let reader = @moonmcap.open_reader(
    @moonmcap.memory_source(@moonmcap.write_indexed(log, messages_per_chunk=8)),
  )
  let reference = @moonmcap.build_query_index(log)
  for descending in [false, true] {
    let query = {
      ..@moonmcap.all_query(),
      topics: ["/shared"],
      descending,
      start_time: 2UL,
      end_time: 20UL,
      offset: 4,
      limit: 13,
    }
    let cursor = reader.cursor(query)
    ignore(cursor.next_page(2))
    cursor.seek(10UL)
    let expected = reference.query({
      ..query,
      offset: 0,
      start_time: if descending {
        2UL
      } else {
        10UL
      },
      end_time: if descending {
        10UL
      } else {
        20UL
      },
    })
    assert_eq(drain_file(cursor, 3), expected)
    let before = cursor.progress()
    try cursor.seek(21UL) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid seek")
    }
    assert_eq(cursor.progress(), before)
  }
}

///|
test "file cursor budget failures are terminal and invalid page is atomic" {
  let reader = @moonmcap.open_reader(
    @moonmcap.memory_source(
      @moonmcap.write_indexed(cursor_fixture(), messages_per_chunk=10),
    ),
  )
  let cursor = reader.cursor(@moonmcap.all_query(), pending_bytes=1)
  try cursor.next() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("pending budget")
  }
  assert_eq(cursor.progress().state, @moonmcap.FileFailed)
  try cursor.next() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("failed cursor reopened")
  }
  let other = reader.cursor(@moonmcap.all_query())
  let before = other.progress()
  try other.next_page(0) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("zero page")
  }
  assert_eq(other.progress(), before)
}

///|
test "nonblocking cursor protocol agrees with synchronous traversal" {
  let bytes = @moonmcap.write_indexed(
    cursor_fixture(),
    compression=@moonmcap.Zstd,
    messages_per_chunk=11,
  )
  let reader = @moonmcap.open_reader(
    @moonmcap.memory_source(bytes),
    cache_bytes=0,
  )
  let cursor = reader.cursor(@moonmcap.all_query())
  let messages = []
  while true {
    match cursor.poll() {
      @moonmcap.FileDone => break
      @moonmcap.FileMessage(message) => messages.push(message)
      @moonmcap.FileRead(span) => {
        let start = span.offset.to_int()
        cursor.feed(bytes[start:start + span.length].to_owned())
      }
    }
  }
  assert_eq(
    messages,
    @moonmcap.build_query_index(cursor_fixture()).query(@moonmcap.all_query()),
  )
  let malformed = reader.cursor(@moonmcap.all_query())
  assert_true(malformed.poll() is @moonmcap.FileRead(_))
  try malformed.feed(b"") catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("short feed")
  }
  assert_eq(malformed.progress().state, @moonmcap.FileFailed)
}

///|
test "unindexed cursor reports full fallback and zero limit avoids it" {
  let bytes = @moonmcap.write_log(example_log())
  let reader = @moonmcap.open_reader(@moonmcap.memory_source(bytes))
  let cursor = reader.cursor({ ..@moonmcap.all_query(), offset: 1, })
  assert_eq(drain_file(cursor, 1).length(), 2)
  assert_true(cursor.progress().used_fallback)
  assert_eq(cursor.progress().skipped_messages, 1)
  let disabled = @moonmcap.open_reader(
    @moonmcap.memory_source(bytes),
    fallback_bytes=0,
  )
  assert_eq(
    disabled.cursor({ ..@moonmcap.all_query(), limit: 0, }).next(),
    None,
  )
}
