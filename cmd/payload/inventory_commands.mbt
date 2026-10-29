///|
fn inventory_file(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  let source = scope.open(values[1])
  let limits = { ..@mcap.default_limits(), max_records: 30000000, }
  ignore(@mcap.scan_source(source, @mcap.source_visitor(), limits~))
  let declarations = @mcap.open_reader(source, limits~)
    .query_source()
    .declaration_log()
  let schemas = declarations.schemas.copy()
  schemas.sort_by(fn(a, b) { a.id.compare(b.id) })
  let channels = declarations.channels.copy()
  channels.sort_by(fn(a, b) { a.id.compare(b.id) })
  let types : Json = {
    "schemas": schemas.map(fn(s) -> Json {
      {
        "id": s.id,
        "name": s.name,
        "encoding": s.encoding,
        "sha256": @nodeio.source_sha256(@mcap.memory_source(s.data)),
      }
    }),
    "channels": channels.map(fn(c) -> Json {
      {
        "id": c.id,
        "topic": c.topic,
        "message_encoding": c.message_encoding,
        "schema_id": c.schema_id,
      }
    }),
  }
  let report : Json = {
    "source_sha256": @nodeio.source_sha256(source),
    "schema_sha256": @nodeio.source_sha256(
      @mcap.memory_source(@utf8.encode(types.stringify()[:])),
    ),
    "types": types,
  }
  @nodeio.write_atomic_new(
    values[2],
    @utf8.encode((report.stringify() + "\n")[:]),
  )
  println(({ "output": values[2] } : Json).stringify())
}
