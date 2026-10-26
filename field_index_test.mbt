///|
test "field zones skip only impossible chunks and retain exact filtered pagination" {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/numbers", "json")
  for i = 0; i < 200; i = i + 1 {
    builder.append(
      id,
      (i % 17).to_uint64(),
      @utf8.encode("{\"n\":\{i},\"text\":\"group-\{i / 20}\"}"[:]),
    )
  }
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let source = @moonmcap.memory_source(
      @moonmcap.write_indexed(
        builder.snapshot(),
        compression~,
        messages_per_chunk=10,
      ),
    )
    let index = @moonmcap.build_field_index(
      source,
      ["/n", "/text", "/missing"],
      index_test_fingerprint,
      "standard-v1",
    )
    let index = @moonmcap.open_field_index(
      source,
      index.to_bytes(),
      index_test_fingerprint,
      "standard-v1",
    )
    for
      expression in [
        "{\"path\":\"/n\",\"op\":\">=\",\"value\":180}", "{\"path\":\"/n\",\"op\":\"=\",\"value\":500}",
        "{\"path\":\"/text\",\"op\":\"=\",\"value\":\"group-9\"}", "{\"not\":{\"path\":\"/missing\",\"op\":\"exists\"}}",
      ] {
      let filter = @moonmcap.json_filter(expression)
      let plan = @moonmcap.open_reader(source, cache_bytes=0).plan_field_query(
        index, filter, index_test_fingerprint, "standard-v1",
      )
      let fast = @moonmcap.open_dataset([plan.query_source()])
      let expected = @moonmcap.open_dataset([
        @moonmcap.build_query_index(builder.snapshot())
        .query_source()
        .decode_payload(@moonmcap.standard_payload_decoder())
        .filter_json(filter),
      ])
      for descending in [false, true] {
        let query = {
          ..@moonmcap.all_query(),
          descending,
          offset: 2,
          limit: 11,
        }
        assert_eq(
          fast.cursor(query).next_page(20),
          expected.cursor(query).next_page(20),
        )
      }
      if expression.contains("500") {
        assert_true(
          plan.info() is { "skipped_chunks": 20, "decoded_chunks": 0, .. },
        )
      }
      if expression.contains("180") {
        assert_true(plan.info() is { "skipped_chunks": 18, .. })
      }
    }
  }
}

///|
test "field index rejects stale configuration corruption and construction budgets" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/raw", "json")
  builder.append(channel, 1UL, b"{\"n\":9007199254740993}")
  builder.append(channel, 2UL, b"invalid-json")
  let source = @moonmcap.memory_source(builder.finish(messages_per_chunk=2))
  let index = @moonmcap.build_field_index(
    source,
    ["/n"],
    index_test_fingerprint,
    "v1",
  )
  assert_true(index.info() is { "unknown_zones": 1, .. })
  let plan = @moonmcap.open_reader(source).plan_field_query(
    index,
    @moonmcap.json_filter(
      "{\"path\":\"/n\",\"op\":\">\",\"value\":999999999999999999}",
    ),
    index_test_fingerprint,
    "v1",
  )
  assert_true(plan.info() is { "skipped_chunks": 0, .. })
  try
    @moonmcap.open_field_index(
      source,
      index.to_bytes(),
      index_test_fingerprint,
      "changed",
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("decoder identity mismatch accepted")
  }
  let corrupt = index.to_bytes().to_array()
  corrupt[corrupt.length() - 1] = corrupt[corrupt.length() - 1] ^ 0x01
  try
    @moonmcap.open_field_index(
      source,
      Bytes::from_array(corrupt),
      index_test_fingerprint,
      "v1",
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("corrupt field index accepted")
  }
  try
    @moonmcap.build_field_index(
      source,
      ["/n", "/n"],
      index_test_fingerprint,
      "v1",
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("duplicate indexed field accepted")
  }
}
