///|
pub fn AsyncQuerySource::decode_payload(
  self : AsyncQuerySource,
  decoder : @mcap.PayloadDecoder,
  max_bytes~ : Int32 = 1024 * 1024,
) -> AsyncQuerySource raise @mcap.McapError {
  if max_bytes < 1 {
    raise @mcap.io_error("invalid decoded JSON byte budget")
  }
  let declarations = {
    ..@mcap.copy_log(self.declarations),
    schemas: [],
    channels: self.declarations.channels.map(fn(c) {
      { ..c, schema_id: 0, message_encoding: "json", }
    }),
  }
  async_query_source(
    declarations,
    fn(query, budget) {
      let stream = (self.open)(query, budget)
      async_message_stream(
        async fn() {
          try {
            match (stream.next)() {
              None => {
                (stream.stop)()
                None
              }
              Some(message) => {
                let decoded = decoder.decode_message(
                  message,
                  self.declarations,
                  max_bytes~,
                )
                if budget < 31 || decoded.data.length() > budget - 31 {
                  raise @mcap.io_error(
                    "decoded message exceeds provider budget",
                  )
                }
                Some(decoded)
              }
            }
          } catch {
            error => {
              (stream.stop)()
              raise error
            }
          }
        },
        stream.stop,
        pending_bytes=stream.pending,
      )
    },
    materialized=self.materialized,
  )
}
