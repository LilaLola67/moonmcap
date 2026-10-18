///|
// Deterministic test callback only; production adapters must supply real SHA-256.
fn index_test_fingerprint(
  source : @moonmcap.ReadSource,
) -> String raise @moonmcap.McapError {
  let word = @moonmcap.encode_hex(
    @moonmcap.crc32(source.read_exact(0UL, source.size().to_int())).to_le_bytes(),
  )
  word + word + word + word + word + word + word + word
}

///|
fn drain_sidecar(
  cursor : @moonmcap.SidecarCursor,
) -> Array[@moonmcap.Message] raise @moonmcap.McapError {
  let messages = []
  while cursor.next() is Some(message) {
    messages.push(message)
  }
  messages
}

///|
test "sidecar queries match memory references across indexed unindexed and sequential layouts" {
  let log = cursor_fixture()
  let expected = @moonmcap.build_query_index(log)
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    for
      bytes in [
        @moonmcap.write_log(log),
        @moonmcap.write_chunked(log, compression~, messages_per_chunk=7),
        @moonmcap.write_indexed(log, compression~, messages_per_chunk=7),
      ] {
      let source = @moonmcap.memory_source(bytes)
      let index = @moonmcap.build_sidecar(
        source,
        index_test_fingerprint,
        identity="test-stable-file",
      )
      let reader = @moonmcap.open_sidecar(
        source,
        index,
        @moonmcap.VerifyContent(index_test_fingerprint),
      )
      reader.validate_entries()
      for descending in [false, true] {
        for topics in [[], ["/shared"], ["/missing"]] {
          let query = {
            ..@moonmcap.all_query(),
            descending,
            topics,
            start_time: 2UL,
            end_time: 20UL,
            offset: 3,
            limit: 37,
          }
          let cursor = reader.cursor(query)
          assert_eq(
            cursor.next_page(2) + drain_sidecar(cursor),
            expected.query(query),
          )
          cursor.seek(10UL)
          assert_eq(
            drain_sidecar(cursor),
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
  }
}

///|
test "sidecar rejects damaged indexes stale content stale identity and budgets" {
  let bytes = @moonmcap.write_log(example_log())
  let source = @moonmcap.memory_source(bytes)
  let index = @moonmcap.build_sidecar(
    source,
    index_test_fingerprint,
    identity="old",
  )
  let changed = bytes.to_array()
  changed[30] = changed[30] ^ 0x01
  try
    @moonmcap.open_sidecar(
      @moonmcap.memory_source(Bytes::from_array(changed)),
      index,
      @moonmcap.VerifyContent(index_test_fingerprint),
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("stale content")
  }
  try
    @moonmcap.open_sidecar(source, index, @moonmcap.TrustIdentity("new"))
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("stale identity")
  }
  for point in [0, 8, index.length() - 1] {
    let damaged = index.to_array()
    damaged[point] = damaged[point] ^ 0x01
    try
      @moonmcap.open_sidecar(
        source,
        Bytes::from_array(damaged),
        @moonmcap.TrustIdentity("old"),
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("damaged sidecar")
    }
  }
  try
    @moonmcap.build_sidecar(source, index_test_fingerprint, index_bytes=128)
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("index budget")
  }
  let fast = @moonmcap.open_sidecar(
    source,
    index,
    @moonmcap.TrustIdentity("old"),
  )
  fast.validate_entries()
  assert_true(fast.info().stringify().contains("\"content_verified\":false"))
}

///|
test "sidecar zero limit seek and cancellation do not read payloads unnecessarily" {
  let bytes = @moonmcap.write_log(cursor_fixture())
  let count = [0]
  let source = @moonmcap.new_read_source(bytes.length().to_uint64(), fn(
    offset,
    length,
  ) {
    count[0] += 1
    bytes[offset.to_int():offset.to_int() + length].to_owned()
  })
  let index = @moonmcap.build_sidecar(
    source,
    index_test_fingerprint,
    identity="test",
  )
  let reader = @moonmcap.open_sidecar(
    source,
    index,
    @moonmcap.TrustIdentity("test"),
  )
  count[0] = 0
  assert_eq(reader.cursor({ ..@moonmcap.all_query(), limit: 0, }).next(), None)
  assert_eq(count[0], 0)
  let cursor = reader.cursor({
    ..@moonmcap.all_query(),
    start_time: 1UL,
    end_time: 20UL,
    limit: 3,
  })
  try cursor.seek(0UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("outside seek")
  }
  assert_eq(count[0], 0)
  assert_true(cursor.next() is Some(_))
  cursor.cancel()
  let stopped = count[0]
  assert_eq(cursor.next(), None)
  assert_eq(count[0], stopped)
  try cursor.seek(10UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("closed seek")
  }
}

///|
test "sidecar sources integrate with datasets without materialized fallback" {
  let log = cursor_fixture()
  let source = @moonmcap.memory_source(@moonmcap.write_log(log))
  let index = @moonmcap.build_sidecar(source, index_test_fingerprint)
  let reader = @moonmcap.open_sidecar(
    source,
    index,
    @moonmcap.VerifyContent(index_test_fingerprint),
  )
  let dataset = @moonmcap.open_dataset([
    reader.query_source(),
    reader.query_source(),
  ])
  let query = {
    ..@moonmcap.all_query(),
    descending: true,
    offset: 3,
    limit: 61,
  }
  let cursor = dataset.cursor(query)
  assert_eq(cursor.progress().materialized_sources, 0)
  assert_eq(
    drain_dataset(cursor),
    @moonmcap.build_query_index(@moonmcap.merge_logs([log, log])).query(query),
  )
}
