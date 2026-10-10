///|
test "indexed summaries match physical grouped spans" {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/x", "raw")
  builder.append(id, 1UL, b"x")
  assert_eq(@moonmcap.validate_summary(builder.finish()), 2)
  assert_eq(
    @moonmcap.validate_summary(@moonmcap.write_log(@moonmcap.empty_log())),
    0,
  )
}
