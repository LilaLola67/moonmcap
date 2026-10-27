///|
test "slice prunes unreachable schema and channel declarations" {
  let sliced = @moonmcap.slice_log(example_log(), {
    ..@moonmcap.all_query(),
    topics: ["/events"],
  })
  assert_eq(sliced.schemas.length(), 0)
  assert_eq(sliced.channels.length(), 1)
  assert_eq(sliced.messages.length(), 1)
  assert_eq(@moonmcap.read_log(@moonmcap.write_log(sliced)), sliced)
}

///|
test "merge remaps colliding source identifiers without changing payload meaning" {
  let left = example_log()
  let right = @moonmcap.empty_log()
  right.schemas.push({
    id: 1,
    name: "Other",
    encoding: "jsonschema",
    data: b"[]",
  })
  right.channels.push({
    id: 1,
    schema_id: 1,
    topic: "/other",
    message_encoding: "json",
    metadata: [],
  })
  right.messages.push({
    channel_id: 1,
    sequence: 7U,
    log_time: 15UL,
    publish_time: 15UL,
    data: b"[]",
  })
  let merged = @moonmcap.merge_logs([left, right])
  assert_eq(merged.schemas.length(), 2)
  assert_eq(merged.channels.length(), 3)
  assert_eq(merged.messages.map(fn(m) { m.log_time }), [10UL, 15UL, 20UL, 30UL])
  let index = @moonmcap.build_query_index(merged)
  assert_eq(
    index.query({ ..@moonmcap.all_query(), topics: ["/other"], })[0].data,
    b"[]",
  )
  assert_eq(@moonmcap.merge_logs([left, left]).schemas.length(), 1)
}

///|
test "clock shifting checks underflow and overflow transactionally" {
  let log = example_log()
  assert_eq(@moonmcap.shift_time(log, 10UL).messages[0].log_time, 40UL)
  assert_true(
    (Result::Ok(@moonmcap.shift_time(log, 11UL, backwards=true)) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
  assert_true(
    (Result::Ok(@moonmcap.shift_time(log, 18446744073709551615UL)) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
  assert_eq(log.messages[0].log_time, 30UL)
}
