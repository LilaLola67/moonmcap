///|
test "statistics distinguish recorded order from chronological gaps" {
  let stats = @moonmcap.summarize(example_log())
  assert_eq(stats.message_count, 3)
  assert_eq(stats.payload_bytes, 9UL)
  let temperature = stats.topics[0]
  assert_eq(temperature.first_time, Some(20UL))
  assert_eq(temperature.last_time, Some(30UL))
  assert_eq(temperature.maximum_gap, 10UL)
  assert_eq(temperature.arrival_regressions, 1)
  assert_eq(temperature.sequence_discontinuities, 0)
  assert_true(@moonmcap.stats_report(stats).contains("messages=3"))
}

///|
test "empty channels avoid fabricated time ranges and division by zero" {
  let log = @moonmcap.empty_log()
  log.channels.push({id: 0, schema_id: 0, topic: "/empty", message_encoding: "", metadata: []})
  let stats = @moonmcap.summarize(log)
  assert_eq(stats.topics[0].first_time, None)
  assert_eq(stats.topics[0].frequency_hz, 0.0)
  assert_eq(stats.topics[0].median_gap, 0UL)
}
