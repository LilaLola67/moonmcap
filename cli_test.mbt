///|
test "command parser preserves UInt64 time and repeated topics" {
  let options = @moonmcap.parse_command([
    "query", "input.mcap", "--topic", "/a", "--topic", "/b", "--start", "18446744073709551614",
    "--end", "18446744073709551615", "--descending", "--offset", "3", "--limit",
    "8", "--format", "csv",
  ])
  assert_eq(options.query.topics, ["/a", "/b"])
  assert_eq(options.query.start_time, 18446744073709551614UL)
  assert_eq(options.query.end_time, 18446744073709551615UL)
  assert_true(options.query.descending)
  assert_eq(options.query.offset, 3)
  assert_eq(options.query.limit, 8)
  assert_eq(options.format, "csv")
  assert_eq(@moonmcap.parse_command(["query", "--", "--input"]).inputs, [
    "--input",
  ])
}

///|
test "command parser rejects invalid options overflow and missing output" {
  for
    args in [
      ["query"],
      ["unknown", "file"],
      ["query", "f", "--typo", "1"],
      ["query", "f", "--start"],
      ["query", "f", "--start", "-1"],
      ["query", "f", "--end", "18446744073709551616"],
      ["query", "f", "--limit", "2147483648"],
      ["query", "f", "--start", "2", "--end", "1"],
      ["query", "f", "--limit", "1", "--limit", "2"],
      ["slice", "f"],
      ["rewrite", "f", "--output", "x", "--compression", "gzip"],
      ["merge", "a", "--output", "x"],
      ["query", "f", "--chunk-messages", "0"],
      ["slice", "f", "--output", "x", "--format", "json"],
      ["diff", "a", "b", "--compression", "zstd"],
      ["job-clean", "job", "--output", "x"],
      ["job-inspect", "job", "--compression", "none"],
      ["index-query", "f", "--index", "i", "--max-file-bytes", "1"],
      ["dataset-query", "f", "--index", "i", "--max-file-bytes", "1"],
      ["recover-blocks", "f", "--output", "x", "--max-file-bytes", "1"],
      ["dataset-query", "f", "--index-bytes", "1"],
    ] {
    try @moonmcap.parse_command(args) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid command accepted")
    }
  }
}

///|
test "CSV quotes multiline topics and JSON exports UInt64 clocks as strings" {
  let log = @moonmcap.empty_log()
  log.channels.push({
    id: 1,
    schema_id: 0,
    topic: "/a,\"b\n",
    message_encoding: "raw",
    metadata: [],
  })
  log.messages.push({
    channel_id: 1,
    sequence: 4294967295U,
    log_time: 18446744073709551615UL,
    publish_time: 18446744073709551614UL,
    data: b"\x00\xff",
  })
  assert_eq(
    @moonmcap.messages_csv(log.messages, log.channels),
    "channel_id,topic,sequence,log_time,publish_time,data_hex\r\n1,\"/a,\"\"b\n\",4294967295,18446744073709551615,18446744073709551614,00ff\r\n",
  )
  let expected : Json = [
    {
      "channel_id": 1,
      "topic": "/a,\"b\n",
      "sequence": 4294967295U.to_json(),
      "log_time": "18446744073709551615",
      "publish_time": "18446744073709551614",
      "data_hex": "00ff",
    },
  ]
  assert_eq(@moonmcap.messages_json(log.messages, log.channels), expected)
}
