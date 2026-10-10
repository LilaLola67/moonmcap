///|
test "time index handles unordered input and inclusive bounds" {
  let index = @moonmcap.build_query_index(example_log())
  let query = @moonmcap.all_query()
  let messages = index.query({ ..query, start_time: 20UL, end_time: 30UL })
  assert_eq(messages.map(fn(m) { m.log_time }), [20UL, 30UL])
  assert_eq(index.query({ ..query, topics: ["/events"] }).length(), 1)
  assert_eq(index.query({ ..query, topics: ["/missing"] }).length(), 0)
  let reversed = index.query({ ..query, descending: true, offset: 1, limit: 1 })
  assert_eq(reversed[0].log_time, 20UL)
}

///|
test "index owns snapshot and does not duplicate repeated topic filters" {
  let log = example_log()
  let index = @moonmcap.build_query_index(log)
  log.messages.clear()
  let query = @moonmcap.all_query()
  assert_eq(index.query(query).length(), 3)
  assert_eq(index.query({ ..query, topics: ["/temperature", "/temperature"] }).length(), 2)
  assert_true((try? index.query({ ..query, offset: -1 })) is Err(_))
}
