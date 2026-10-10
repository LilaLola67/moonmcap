///|
test "indexed queries agree with logical index across filters and ordering" {
  let builder = @moonmcap.new_builder()
  let a = builder.register_channel("/a", "raw")
  let b = builder.register_channel("/b", "raw")
  for i = 0; i < 20; i = i + 1 {
    builder.append(if i % 2 == 0 { a } else { b }, (20 - i).to_uint64(), b"x")
  }
  let log = builder.snapshot()
  let bytes = builder.finish(messages_per_chunk=2)
  for descending in [false, true] {
    for topics in [[], ["/a"], ["missing"]] {
      let query = {
        ..@moonmcap.all_query(),
        topics,
        descending,
        start_time: 5UL,
        end_time: 13UL,
        offset: 1,
        limit: 3,
      }
      let result = @moonmcap.query_file(bytes, query)
      assert_eq(result.messages, @moonmcap.build_query_index(log).query(query))
      assert_true(result.diagnostics.used_file_indexes)
    }
  }
}
