///|
/// Physical scan evidence. Accepted records can still fail the final data CRC.
pub(all) enum SourceScanEvent {
  RecordStart(UInt64)
  RecordRead(UInt64, Byte, Int)
  RecordAccepted(UInt64, Byte, Int)
  DataChecksum(UInt64, UInt, UInt)
  DataValidated(UInt64, Bool)
  SummaryStart(UInt64)
}

///|
pub(all) struct MessageLocation {
  offset : UInt64
  span : Int
  inner : Int?
} derive(Eq, @debug.Debug)

///|
pub(all) struct SourceVisitor {
  schema : (Schema) -> Unit raise McapError
  channel : (Channel) -> Unit raise McapError
  message : (Message, MessageLocation) -> Unit raise McapError
  attachment : (Attachment) -> Unit raise McapError
  metadata : (Metadata) -> Unit raise McapError
}

///|
pub fn source_visitor(
  schema? : (Schema) -> Unit raise McapError = fn(_) { () },
  channel? : (Channel) -> Unit raise McapError = fn(_) { () },
  message? : (Message, MessageLocation) -> Unit raise McapError = fn(_, _) {
    ()
  },
  attachment? : (Attachment) -> Unit raise McapError = fn(_) { () },
  metadata? : (Metadata) -> Unit raise McapError = fn(_) { () },
) -> SourceVisitor {
  { schema, channel, message, attachment, metadata, }
}

///|
/// Bounded full validation in physical order. Callbacks precede final CRC validation;
/// callers must discard side effects if validation fails.
pub fn scan_source(
  source : ReadSource,
  visitor : SourceVisitor,
  limits? : Limits = default_limits(),
  declaration_bytes? : Int = 8 * 1024 * 1024,
  cancelled? : () -> Bool = fn() { false },
  trace? : (SourceScanEvent) -> Unit = fn(_) { () },
) -> UInt64 raise McapError {
  if declaration_bytes < 0 {
    raise Invalid("negative scan declaration budget")
  }
  let reader = open_reader(
    source,
    limits~,
    cache_bytes=0,
    summary_bytes=declaration_bytes,
    fallback_bytes=0,
  )
  let storage = sort_storage(
    fn(_) { raise Invalid("scan cannot create sort runs") },
    fn(_) { raise Invalid("scan cannot read sort runs") },
    fn(_) { () },
  )
  let options = { ..external_options(), limits, declaration_bytes, }
  let session : SortSession = {
    observer: Some(visitor),
    trace: Some(trace),
    preserve_ids: true,
    storage,
    options,
    live: Map([]),
    cancelled,
    buffer: [],
    buffered: 0,
    ordinal: 0UL,
    next_id: 0,
    spool: 0UL,
    peak_spool: 0UL,
    peak_run: 0,
    peak_merge: 0,
    passes: 0,
    declared: 0,
  }
  let global = { ..empty_log(), header: reader.declarations.header, }
  let writer = new_stream_writer(
    new_write_sink(fn(_) { () }),
    profile=global.header.profile,
    limits~,
  )
  session.ingest(source, global, writer, [])
  session.ordinal
}
