///|
test "empty logs own separate collections" {
  let first = @moonmcap.empty_log()
  let second = @moonmcap.empty_log()
  first.metadata.push({ name: "run", entries: [("id", "one")] })
  assert_eq(second.metadata.length(), 0)
  assert_eq(@moonmcap.magic().length(), 8)
  assert_true(@moonmcap.default_limits().max_record_bytes > 0)
}
