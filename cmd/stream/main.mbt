///|
extern "js" fn argument(index : Int) -> String =
  #| i => process.argv.slice(2)[i] ?? ''

///|
fn main {
  try {
    let compression = match argument(1) {
      "none" | "" => @mcap.Uncompressed
      "lz4" => @mcap.Lz4
      "zstd" => @mcap.Zstd
      _ => raise @mcap.io_error("unknown compression")
    }
    let writer = @mcap.new_stream_writer(@nodeio.create_sink(argument(0)), options={
      ..@mcap.stream_options(),
      compression,
      chunk_bytes: 16384,
      chunk_messages: 64,
    })
    writer.add_channel({
      id: 1,
      schema_id: 0,
      topic: "/stream",
      message_encoding: "raw",
      metadata: [],
    })
    for i = 0; i < 1000; i = i + 1 {
      writer.append({
        channel_id: 1,
        sequence: i.reinterpret_as_uint(),
        log_time: i.to_uint64(),
        publish_time: i.to_uint64(),
        data: Bytes::make(1024, b'x'),
      })
    }
    writer.add_metadata({ name: "recording", entries: [("mode", "stream")], })
    let p = writer.finish()
    let report : Json = {
      "messages": p.messages.to_string(),
      "chunks": p.chunks,
      "peak_buffered_bytes": p.peak_buffered_bytes,
      "retained_summary_bytes": p.retained_summary_bytes,
      "bytes_written": p.bytes_written.to_string(),
    }
    println(report.stringify())
  } catch {
    @mcap.Invalid(reason) => abort(reason)
  }
}
