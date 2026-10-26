///|
fn disk_poll_next(
  cursor : @moonmcap.DiskSidecarPollCursor,
  source : @moonmcap.ReadSource,
  index : @moonmcap.ReadSource,
) -> @moonmcap.Message? raise @moonmcap.McapError {
  while true {
    match cursor.poll() {
      @moonmcap.DiskDone => return None
      @moonmcap.DiskMessage(message) => return Some(message)
      @moonmcap.DiskIndexRead(span) =>
        cursor.feed_index(index.read_exact(span.offset, span.length))
      @moonmcap.DiskSourceRead(span) =>
        cursor.feed_source(source.read_exact(span.offset, span.length))
    }
  }
  None
}

///|
test "paged poll protocol audits all pages and matches stable reference without synchronous data reads" {
  let log = cursor_fixture()
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let source = @moonmcap.memory_source(
      @moonmcap.write_indexed(log, compression~, messages_per_chunk=11),
    )
    let bytes = @buffer.Buffer::Buffer()
    ignore(
      @moonmcap.build_disk_sidecar(
        source,
        memory_sort_storage(Map([]), [false]),
        @moonmcap.new_write_sink(fn(data) { bytes.write_bytes(data[:]) }),
        index_test_fingerprint,
        page_entries=4,
      ),
    )
    let index = @moonmcap.memory_source(bytes.to_bytes())
    let audit = @moonmcap.start_disk_sidecar_audit(
      source,
      index,
      @moonmcap.VerifyContent(index_test_fingerprint),
      cache_pages=0,
    )
    try audit.finish() catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("unfinished audit accepted")
    }
    let mut pages = 0
    while audit.poll() is Some(span) {
      audit.feed(index.read_exact(span.offset, span.length))
      pages += 1
    }
    assert_true(pages > 1)
    let reader = audit.finish()
    for descending in [false, true] {
      let query = {
        ..@moonmcap.all_query(),
        descending,
        start_time: 3UL,
        end_time: 23UL,
        offset: 3,
        limit: 37,
      }
      let cursor = reader.poll_cursor(query)
      let messages = []
      while disk_poll_next(cursor, source, index) is Some(message) {
        messages.push(message)
      }
      assert_eq(messages, @moonmcap.build_query_index(log).query(query))
      cursor.seek(12UL)
      let restarted = []
      while disk_poll_next(cursor, source, index) is Some(message) {
        restarted.push(message)
      }
      let resumed = {
        ..query,
        offset: 0,
        start_time: if descending {
          query.start_time
        } else {
          12UL
        },
        end_time: if descending {
          12UL
        } else {
          query.end_time
        },
      }
      assert_eq(restarted, @moonmcap.build_query_index(log).query(resumed))
      cursor.cancel()
      assert_true(cursor.poll() is @moonmcap.DiskDone)
    }
  }
}
