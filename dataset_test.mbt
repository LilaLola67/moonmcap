///|
fn drain_dataset(
  cursor : @moonmcap.DatasetCursor,
) -> Array[@moonmcap.Message] raise @moonmcap.McapError {
  let messages = []
  while cursor.next() is Some(message) {
    messages.push(message)
  }
  messages
}

///|
test "dataset pages equal stable physical multi-file reference including seek" {
  let first = cursor_fixture()
  let second = cursor_fixture()
  second.channels[0] = { ..second.channels[0], topic: "/different", }
  let expected = @moonmcap.build_query_index(
    @moonmcap.merge_logs([first, second]),
  )
  let readers = [
    @moonmcap.open_reader(
      @moonmcap.memory_source(
        @moonmcap.write_indexed(
          first,
          compression=@moonmcap.Lz4,
          messages_per_chunk=7,
        ),
      ),
    ),
    @moonmcap.open_reader(
      @moonmcap.memory_source(
        @moonmcap.write_indexed(
          second,
          compression=@moonmcap.Zstd,
          messages_per_chunk=5,
        ),
      ),
    ),
  ]
  let dataset = @moonmcap.open_dataset(
    readers.map(fn(reader) { reader.query_source() }),
  )
  for descending in [false, true] {
    for topics in [[], ["/shared"], ["/different"], ["/missing"]] {
      let query = {
        ..@moonmcap.all_query(),
        descending,
        topics,
        start_time: 2UL,
        end_time: 20UL,
        offset: 7,
        limit: 37,
      }
      let cursor = dataset.cursor(query)
      let first_page = cursor.next_page(3).map(fn(record) { record.message })
      let rest = drain_dataset(cursor)
      assert_eq(first_page + rest, expected.query(query))
      cursor.seek(10UL)
      assert_eq(
        drain_dataset(cursor),
        expected.query({
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
        }),
      )
    }
  }
}

///|
test "dataset keeps duplicates and provenance reverses deterministic equal-time order" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/same", "raw")
  builder.append(channel, 42UL, b"duplicate")
  let bytes = builder.finish()
  let source = @moonmcap.open_reader(@moonmcap.memory_source(bytes)).query_source()
  let dataset = @moonmcap.open_dataset([source, source])
  for descending in [false, true] {
    let cursor = dataset.cursor({ ..@moonmcap.all_query(), descending, })
    let page = cursor.next_page(4)
    assert_eq(page.length(), 2)
    assert_eq(page[0].source_index, if descending { 1 } else { 0 })
    assert_eq(page[1].source_index, if descending { 0 } else { 1 })
    assert_eq(page[0].message, page[1].message)
  }
  let out = @buffer.Buffer::Buffer()
  assert_eq(
    dataset
    .cursor(@moonmcap.all_query())
    .write_ndjson(@moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) })),
    2,
  )
}

///|
test "dataset zero limit lazy loading and cancellation avoid further reads" {
  let bytes = @moonmcap.write_indexed(cursor_fixture(), messages_per_chunk=8)
  let source = @moonmcap.memory_source(bytes)
  let reader = @moonmcap.open_reader(source, cache_bytes=0)
  let dataset = @moonmcap.open_dataset([
    reader.query_source(),
    reader.query_source(),
  ])
  let before = reader.diagnostics().bytes_read
  assert_eq(dataset.cursor({ ..@moonmcap.all_query(), limit: 0, }).next(), None)
  let cursor = dataset.cursor({ ..@moonmcap.all_query(), limit: 3, })
  assert_eq(reader.diagnostics().bytes_read, before)
  ignore(cursor.next())
  cursor.cancel()
  let stopped = reader.diagnostics().bytes_read
  assert_eq(cursor.next(), None)
  assert_eq(reader.diagnostics().bytes_read, stopped)
  try cursor.seek(0UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("cancelled seek")
  }
  let tiny = dataset.cursor(@moonmcap.all_query(), pending_bytes=1)
  try tiny.next() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("tiny budget")
  }
  assert_eq(tiny.progress().state, @moonmcap.FileFailed)
}

///|
test "dataset rejects incompatible profiles and marks unindexed materialization" {
  let log = example_log()
  let first = @moonmcap.open_reader(
    @moonmcap.memory_source(@moonmcap.write_log(log)),
  ).query_source()
  let other = { ..log, header: { ..log.header, profile: "different", }, }
  let second = @moonmcap.open_reader(
    @moonmcap.memory_source(@moonmcap.write_log(other)),
  ).query_source()
  try @moonmcap.open_dataset([first, second]) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("profile mismatch")
  }
  try @moonmcap.open_dataset([first], declaration_bytes=0) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("declaration budget")
  }
  let cursor = @moonmcap.open_dataset([first]).cursor(@moonmcap.all_query())
  assert_eq(cursor.progress().materialized_sources, 1)
  assert_eq(
    drain_dataset(cursor),
    @moonmcap.build_query_index(@moonmcap.merge_logs([log])).query(
      @moonmcap.all_query(),
    ),
  )
}
