///|
fn cursor_fixture() -> @moonmcap.Log {
  let log = @moonmcap.empty_log()
  for id = 1; id <= 9; id = id + 1 {
    log.channels.push({
      id,
      schema_id: 0,
      topic: if id <= 3 {
        "/shared"
      } else {
        "/topic/\{id}"
      },
      message_encoding: "raw",
      metadata: [],
    })
  }
  // Deliberately unordered; channel 9 has no messages. Repeated times cross pages.
  for i = 0; i < 257; i = i + 1 {
    log.messages.push({
      channel_id: i * 7 % 8 + 1,
      sequence: i.reinterpret_as_uint(),
      log_time: (i * 37 % 23).to_uint64(),
      publish_time: i.to_uint64(),
      data: b"payload",
    })
  }
  log
}

///|
fn collect_cursor(
  cursor : @moonmcap.QueryCursor,
  page_size : Int32,
) -> Array[@moonmcap.Message] raise @moonmcap.McapError {
  let result = []
  while true {
    let page = cursor.next_page(page_size)
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
test "cursor pages agree with batch query across ordering topics and limits" {
  let index = @moonmcap.build_query_index(cursor_fixture())
  for
    topics in [
      [],
      ["/shared"],
      ["/shared", "/topic/5", "/shared"],
      ["/missing"],
    ] {
    for descending in [false, true] {
      for offset in [0, 1, 13, 300] {
        for limit in [0, 1, 31, 1000] {
          let query = {
            ..@moonmcap.all_query(),
            topics,
            start_time: 3UL,
            end_time: 19UL,
            descending,
            offset,
            limit,
          }
          let expected = index.query(query)
          for page_size in [1, 7, 64] {
            let cursor = index.cursor(query)
            assert_eq(collect_cursor(cursor, page_size), expected)
            let progress = cursor.progress()
            assert_eq(progress.state, @moonmcap.Exhausted)
            assert_eq(progress.emitted_messages, expected.length())
            assert_eq(progress.remaining_messages, 0)
            assert_eq(progress.buffered_heads, 0)
          }
        }
      }
    }
  }
}

///|
test "equal timestamps preserve physical order across channels and pages" {
  let log = cursor_fixture()
  log.messages.clear()
  for i = 0; i < 50; i = i + 1 {
    log.messages.push({
      channel_id: i % 3 + 1,
      sequence: i.reinterpret_as_uint(),
      log_time: 18446744073709551615UL,
      publish_time: 0UL,
      data: b"",
    })
  }
  let index = @moonmcap.build_query_index(log)
  for descending in [false, true] {
    let cursor = index.cursor({
      ..@moonmcap.all_query(),
      topics: ["/shared"],
      descending,
      offset: 3,
      limit: 41,
    })
    let result = collect_cursor(cursor, 4)
    assert_eq(result.length(), 41)
    for i = 0; i < result.length(); i = i + 1 {
      let expected = if descending { 46 - i } else { i + 3 }
      assert_eq(result[i].sequence, expected.reinterpret_as_uint())
    }
  }
}

///|
test "cursor progress reports matched skipped emitted and bounded heads" {
  let index = @moonmcap.build_query_index(cursor_fixture())
  let query = {
    ..@moonmcap.all_query(),
    topics: ["/shared"],
    offset: 2,
    limit: 9,
  }
  let cursor = index.cursor(query)
  let matched = index.query({ ..query, offset: 0, limit: 1000, }).length()
  assert_eq(cursor.progress(), {
    state: @moonmcap.Active,
    matching_messages: matched,
    skipped_messages: 2,
    emitted_messages: 0,
    remaining_messages: 9,
    buffered_heads: 3,
  })
  assert_eq(cursor.next_page(4).length(), 4)
  assert_eq(cursor.progress().remaining_messages, 5)
  assert_eq(cursor.progress().emitted_messages, 4)
  assert_true(cursor.progress().buffered_heads <= 3)
  assert_true(cursor.next() is Some(_))
  assert_eq(cursor.next_page(100).length(), 4)
  assert_eq(cursor.progress().emitted_messages, 9)
  assert_eq(cursor.next(), None)
  assert_eq(cursor.next_page(3), [])
  let all = index.cursor(@moonmcap.all_query())
  assert_eq(all.progress().buffered_heads, 1)
}

///|
test "seek restarts inclusively in direction without reapplying offset" {
  let index = @moonmcap.build_query_index(cursor_fixture())
  for descending in [false, true] {
    let query : @moonmcap.Query = {
      topics: ["/shared", "/topic/7"],
      start_time: 2UL,
      end_time: 20UL,
      descending,
      offset: 10,
      limit: 50,
    }
    let cursor = index.cursor(query)
    ignore(cursor.next_page(3))
    for time in [10UL, 2UL, 20UL, 7UL] {
      cursor.seek(time)
      assert_eq(cursor.progress().skipped_messages, 0)
      assert_eq(cursor.progress().emitted_messages, 0)
      let expected = index.query({
        ..query,
        start_time: if descending {
          query.start_time
        } else {
          time
        },
        end_time: if descending {
          time
        } else {
          query.end_time
        },
        offset: 0,
      })
      assert_eq(collect_cursor(cursor, 3), expected)
    }
  }
}

///|
test "invalid seek and page size are atomic and cancellation is terminal" {
  let index = @moonmcap.build_query_index(cursor_fixture())
  let query = { ..@moonmcap.all_query(), start_time: 5UL, end_time: 17UL, }
  let cursor = index.cursor(query)
  let before = cursor.progress()
  try cursor.next_page(0) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("zero page size must fail")
  }
  try cursor.next_page(-1) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("negative page size must fail")
  }
  try cursor.seek(4UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("seek below query interval must fail")
  }
  try cursor.seek(18UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("seek above query interval must fail")
  }
  assert_eq(cursor.progress(), before)
  assert_eq(cursor.next(), Some(index.query(query)[0]))
  cursor.cancel()
  cursor.cancel()
  assert_eq(cursor.progress().state, @moonmcap.Cancelled)
  assert_eq(cursor.progress().emitted_messages, 1)
  assert_eq(cursor.progress().remaining_messages, 0)
  assert_eq(cursor.progress().buffered_heads, 0)
  assert_eq(cursor.next(), None)
  assert_eq(cursor.next_page(4), [])
  try cursor.seek(10UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("cancelled cursor must not reopen")
  }
  assert_eq(cursor.progress().state, @moonmcap.Cancelled)
}

///|
test "cursor shares stable index snapshot and copies topic filters" {
  let log = cursor_fixture()
  let index = @moonmcap.build_query_index(log)
  let topics = ["/shared"]
  let query = { ..@moonmcap.all_query(), topics, }
  let expected = index.query(query)
  let first = index.cursor(query)
  let second = index.cursor(query)
  topics.clear()
  topics.push("/missing")
  log.messages.clear()
  log.channels.clear()
  let page = first.next_page(2)
  page.clear()
  first.seek(0UL)
  assert_eq(collect_cursor(first, 5), expected)
  assert_eq(collect_cursor(second, 8), expected)
}

///|
test "empty cursors zero limits and extreme offsets are well defined" {
  let empty = @moonmcap.build_query_index(@moonmcap.empty_log())
  let cursor = empty.cursor(@moonmcap.all_query())
  assert_eq(cursor.next(), None)
  cursor.seek(18446744073709551615UL)
  assert_eq(cursor.progress().state, @moonmcap.Exhausted)
  let index = @moonmcap.build_query_index(cursor_fixture())
  let beyond = index.cursor({ ..@moonmcap.all_query(), offset: 2147483647, })
  assert_eq(beyond.progress().skipped_messages, 257)
  assert_eq(beyond.next(), None)
  beyond.seek(0UL)
  assert_eq(beyond.progress().remaining_messages, 257)
  let zero = index.cursor({ ..@moonmcap.all_query(), limit: 0, })
  zero.seek(0UL)
  assert_eq(zero.progress().state, @moonmcap.Exhausted)
  let absent = index.cursor({ ..@moonmcap.all_query(), topics: ["/topic/9"], })
  assert_eq(absent.progress().matching_messages, 0)
}

///|
test "invalid cursor query rejected before traversal" {
  let index = @moonmcap.build_query_index(cursor_fixture())
  for
    query in [
      { ..@moonmcap.all_query(), offset: -1, },
      { ..@moonmcap.all_query(), limit: -1, },
      { ..@moonmcap.all_query(), start_time: 10UL, end_time: 9UL, },
    ] {
    try index.cursor(query) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid query must fail")
    }
  }
}

///|
test "cursor results survive round trips through all compression modes" {
  let log = cursor_fixture()
  let query = {
    ..@moonmcap.all_query(),
    topics: ["/shared"],
    descending: true,
  }
  let expected = @moonmcap.build_query_index(log).query(query)
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let bytes = @moonmcap.write_indexed(log, messages_per_chunk=7, compression~)
    let index = @moonmcap.build_query_index(@moonmcap.read_log(bytes))
    assert_eq(collect_cursor(index.cursor(query), 11), expected)
  }
}

///|
test "seeded random cursor traversal matches independent stable sort oracle" {
  let mut seed = 20261017U
  for width in [1, 2, 3, 8, 17, 32] {
    let log = @moonmcap.empty_log()
    for id = 1; id <= width; id = id + 1 {
      log.channels.push({
        id,
        schema_id: 0,
        topic: "/group/\{id % 3}",
        message_encoding: "raw",
        metadata: [],
      })
    }
    for i = 0; i < 1000; i = i + 1 {
      seed = seed * 1664525U + 1013904223U
      let id = (seed % width.reinterpret_as_uint()).reinterpret_as_int() + 1
      seed = seed * 1664525U + 1013904223U
      let time = (seed % 101U).to_uint64()
      log.messages.push({
        channel_id: id,
        sequence: i.reinterpret_as_uint(),
        log_time: time,
        publish_time: time,
        data: b"random-fixture",
      })
    }
    let index = @moonmcap.build_query_index(log)
    for descending in [false, true] {
      for group in [-1, 0, 1, 2] {
        let expected = log.messages.filter(fn(message) {
          message.log_time >= 13UL &&
          message.log_time <= 87UL &&
          (group < 0 || message.channel_id % 3 == group)
        })
        expected.sort_by(fn(a, b) {
          let order = if a.log_time == b.log_time {
            a.sequence.compare(b.sequence)
          } else {
            a.log_time.compare(b.log_time)
          }
          if descending {
            -order
          } else {
            order
          }
        })
        let offset = expected.length().min(11)
        let count = (expected.length() - offset).min(103)
        let oracle = Array::makei(count, fn(i) { expected[offset + i] })
        let cursor = index.cursor({
          topics: if group < 0 {
            []
          } else {
            ["/group/\{group}"]
          },
          start_time: 13UL,
          end_time: 87UL,
          descending,
          offset: 11,
          limit: 103,
        })
        assert_true(cursor.progress().buffered_heads <= width)
        assert_eq(collect_cursor(cursor, 13), oracle)
      }
    }
  }
}
