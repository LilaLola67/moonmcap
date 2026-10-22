///|
test "sidecar poll feed matches synchronous order and rejects unsolicited input" {
  let log = cursor_fixture()
  let bytes = @moonmcap.write_indexed(
    log,
    compression=@moonmcap.Zstd,
    messages_per_chunk=5,
  )
  let source = @moonmcap.memory_source(bytes)
  let index = @moonmcap.build_sidecar(
    source,
    fn(_) { "0000000000000000000000000000000000000000000000000000000000000000" },
    identity="test",
  )
  let reader = @moonmcap.open_sidecar(
    source,
    index,
    @moonmcap.TrustIdentity("test"),
    cache_bytes=0,
  )
  for descending in [false, true] {
    let query = { ..@moonmcap.all_query(), descending, offset: 5, limit: 17, }
    let cursor = reader.cursor(query)
    let actual = []
    while true {
      match cursor.poll() {
        @moonmcap.FileDone => break
        @moonmcap.FileMessage(message) => actual.push(message)
        @moonmcap.FileRead(span) =>
          cursor.feed(source.read_exact(span.offset, span.length))
      }
    }
    assert_eq(actual, @moonmcap.build_query_index(log).query(query))
  }
  let cursor = reader.cursor(@moonmcap.all_query())
  try cursor.feed(b"bad") catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("unsolicited sidecar feed")
  }
  try cursor.poll() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("failed cursor reused")
  }
}
