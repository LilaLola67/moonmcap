///|
test "disk index pages preserve physical ties across layouts and bounded cache" {
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
      let files : Map[Int32, @buffer.Buffer] = Map([])
      let closed = [false]
      let out = @buffer.Buffer::Buffer()
      ignore(
        @moonmcap.build_disk_sidecar(
          source,
          memory_sort_storage(files, closed),
          @moonmcap.new_write_sink(fn(bytes) { out.write_bytes(bytes[:]) }),
          index_test_fingerprint,
          page_entries=5,
          options={ ..@moonmcap.external_options(), run_bytes: 512, fan_in: 3, },
        ),
      )
      assert_true(files.is_empty() && closed[0])
      let reader = @moonmcap.open_disk_sidecar(
        source,
        @moonmcap.memory_source(out.to_bytes()),
        @moonmcap.VerifyContent(index_test_fingerprint),
        cache_pages=2,
      )
      reader.validate_entries()
      for descending in [false, true] {
        let query = {
          ..@moonmcap.all_query(),
          descending,
          start_time: 2UL,
          end_time: 20UL,
          offset: 3,
          limit: 37,
        }
        let cursor = reader.cursor(query)
        assert_eq(
          cursor.next_page(2) + cursor.next_page(100),
          expected.query(query),
        )
        cursor.seek(10UL)
        let sought = if descending {
          { ..query, offset: 0, end_time: 10UL, }
        } else {
          { ..query, offset: 0, start_time: 10UL, }
        }
        assert_eq(cursor.next_page(100), expected.query(sought))
        cursor.cancel()
        assert_true(cursor.next() is None)
      }
      assert_true(
        reader.info()
        is { "peak_cached_pages": 2, "content_verified": true, .. },
      )
      let dataset = @moonmcap.open_dataset([reader.query_source()])
      assert_eq(
        dataset
        .cursor(@moonmcap.all_query())
        .next_page(1000)
        .map(fn(r) { r.message.log_time }),
        expected.query(@moonmcap.all_query()).map(fn(m) { m.log_time }),
      )
    }
  }
}

///|
test "disk index refuses damaged header pages reordered CRC-valid pages and stale sources" {
  let log = cursor_fixture()
  let bytes = @moonmcap.write_indexed(log)
  let source = @moonmcap.memory_source(bytes)
  let out = @buffer.Buffer::Buffer()
  ignore(
    @moonmcap.build_disk_sidecar(
      source,
      memory_sort_storage(Map([]), [false]),
      @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }),
      index_test_fingerprint,
      page_entries=4,
    ),
  )
  let original = out.to_bytes()
  let mut length = 0
  for i = 0; i < 4; i = i + 1 {
    length = length | (original[8 + i].to_int() << (i * 8))
  }
  let start = 16 + length
  let span = 4 * 36 + 4
  for point in [12, start, original.length() - 1] {
    let damaged = original.to_array()
    damaged[point] = damaged[point] ^ 0x40
    try
      @moonmcap.open_disk_sidecar(
        source,
        @moonmcap.memory_source(Bytes::from_array(damaged)),
        @moonmcap.VerifyContent(index_test_fingerprint),
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("damaged disk index accepted")
    }
  }
  let swapped = original[0:start].to_owned() +
    original[start + span:start + span * 2].to_owned() +
    original[start:start + span].to_owned() +
    original[start + span * 2:].to_owned()
  try
    @moonmcap.open_disk_sidecar(
      source,
      @moonmcap.memory_source(swapped),
      @moonmcap.VerifyContent(index_test_fingerprint),
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("CRC-valid out-of-order pages accepted")
  }
  let changed = bytes.to_array()
  changed[30] = changed[30] ^ 0x01
  try
    @moonmcap.open_disk_sidecar(
      @moonmcap.memory_source(Bytes::from_array(changed)),
      @moonmcap.memory_source(original),
      @moonmcap.VerifyContent(index_test_fingerprint),
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("stale source accepted")
  }
}

///|
test "disk index total spool failures and cancellation clean dedicated storage" {
  let bytes = @moonmcap.write_indexed(cursor_fixture())
  for cancel in [false, true] {
    let files : Map[Int32, @buffer.Buffer] = Map([])
    let closed = [false]
    let calls = [0]
    try
      @moonmcap.build_disk_sidecar(
        @moonmcap.memory_source(bytes),
        memory_sort_storage(files, closed),
        @moonmcap.new_write_sink(fn(_) { () }),
        index_test_fingerprint,
        options={
          ..@moonmcap.external_options(),
          spool_bytes: if cancel {
            1000000UL
          } else {
            50UL
          },
        },
        cancelled=fn() {
          calls[0] += 1
          cancel && calls[0] > 3
        },
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("disk index failure expected")
    }
    assert_true(files.is_empty() && closed[0])
  }
}
