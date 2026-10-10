///|
test "attachments survive layouts transforms and independent checksums" {
  let log = @moonmcap.empty_log()
  let attachment : @moonmcap.Attachment = { log_time: 10UL, create_time: 8UL, name: "calibration", media_type: "application/json", data: b"{}" }
  log.attachments.push(attachment)
  assert_eq(@moonmcap.decode_attachment(@moonmcap.encode_attachment(attachment)), attachment)
  for bytes in [@moonmcap.write_log(log), @moonmcap.write_chunked(log), @moonmcap.write_indexed(log)] {
    assert_eq(@moonmcap.read_log(bytes), log)
  }
  assert_eq(@moonmcap.merge_logs([log, log]).attachments.length(), 2)
  assert_eq(@moonmcap.slice_log(log, @moonmcap.all_query()).attachments.length(), 1)
  assert_eq(@moonmcap.shift_time(log, 2UL).attachments[0].create_time, 10UL)
  let body = @moonmcap.encode_attachment(attachment)
  let broken = b"\x0b" + body[1:].to_owned()
  let rejected = try @moonmcap.decode_attachment(broken) catch { _ => true } noraise { _ => false }
  assert_true(rejected)
}
