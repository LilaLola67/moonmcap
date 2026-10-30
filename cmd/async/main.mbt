///|
extern "js" fn argument(index : Int32) -> String =
  #| i => process.argv.slice(2)[i] ?? ''

///|
extern "js" fn probe_start() -> Unit =
  #| () => { globalThis.__moonmcapTicks=0; globalThis.__moonmcapTimer=setInterval(()=>globalThis.__moonmcapTicks++,1); globalThis.__moonmcapTimer.unref(); }

///|
extern "js" fn probe_stop() -> Int32 =
  #| () => { clearInterval(globalThis.__moonmcapTimer); return globalThis.__moonmcapTicks; }

///|
extern "js" fn report_error(message : String) -> Unit =
  #| message => { console.error(message); process.exitCode=2; }

///|
async fn main {
  try {
    let mode = argument(0)
    if mode == "remote-cancel" {
      let source = @aio.open_http(argument(1), timeout_ms=5000, retries=0)
      let observed = [false]
      @async.with_task_group(async fn(group) {
        let task = group.spawn(
          async fn() { ignore(source.read_exact(0UL, 8)) },
          allow_failure=true,
        )
        @async.sleep(10)
        source.cancel()
        task.wait() catch {
          _ => observed[0] = true
        }
      })
      let report : Json = {
        "cancelled": source.metrics().cancelled,
        "read_aborted": observed[0],
      }
      println(report.stringify())
      return
    }
    if mode == "query" || mode == "remote" {
      let source = if mode == "remote" {
        let timeout = if argument(5) == "" {
          5000UL
        } else {
          @mcap.parse_decimal(argument(5))
        }
        let retries = if argument(6) == "" {
          2UL
        } else {
          @mcap.parse_decimal(argument(6))
        }
        if timeout > 2147483647UL || retries > 10UL {
          raise @mcap.io_error("HTTP option exceeds budget")
        }
        @aio.open_http(
          argument(1),
          timeout_ms=timeout.to_int(),
          retries=retries.to_int(),
        )
      } else {
        @aio.open_file(argument(1))
      }
      let reader = @aio.open_reader(source)
      let query = {
        ..@mcap.all_query(),
        topics: [argument(2)],
        start_time: @mcap.parse_decimal(argument(3)),
        end_time: @mcap.parse_decimal(argument(4)),
      }
      let cursor = reader.cursor(query)
      let messages = []
      while true {
        let page = cursor.next_page(3)
        if page.is_empty() {
          break
        }
        for message in page {
          messages.push(message)
        }
      }
      let m = source.metrics()
      let report : Json = {
        "messages": @mcap.messages_json(messages, reader.channels()),
        "bytes_read": m.bytes_read.to_string(),
        "loaded_chunks": cursor.progress().loaded_chunks,
      }
      println(report.stringify())
      return
    }
    let compression = match argument(2) {
      "none" | "" => @mcap.Uncompressed
      "lz4" => @mcap.Lz4
      "zstd" => @mcap.Zstd
      _ => raise @mcap.io_error("unknown async compression")
    }
    let options = {
      ..@mcap.stream_options(),
      compression,
      chunk_bytes: 16384,
      chunk_messages: 64,
    }
    probe_start()
    if mode == "record" {
      let writer = @aio.new_writer(@aio.create_sink(argument(1)), options~)
      writer.add_channel({
        id: 1,
        schema_id: 0,
        topic: "/async",
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
      let p = writer.finish()
      let report : Json = {
        "messages": p.encoded.messages.to_string(),
        "bytes_written": p.bytes_written.to_string(),
        "peak_queued_bytes": p.peak_queued_bytes,
        "event_loop_ticks": probe_stop(),
      }
      println(report.stringify())
    } else if mode == "rotate" {
      let template = @mcap.empty_log()
      template.channels.push({
        id: 1,
        schema_id: 0,
        topic: "/async",
        message_encoding: "raw",
        metadata: [],
      })
      let writer = @aio.rotating_writer(
        template,
        async fn(index) { @aio.create_sink("\{argument(1)}.\{index}.mcap") },
        options~,
        segment_messages=32,
      )
      for i = 0; i < 100; i = i + 1 {
        writer.append({
          channel_id: 1,
          sequence: i.reinterpret_as_uint(),
          log_time: i.to_uint64(),
          publish_time: i.to_uint64(),
          data: b"segment",
        })
      }
      let segments = writer.finish()
      let report : Json = {
        "segments": segments.map(fn(s) -> Json {
          {
            "ordinal": s.ordinal,
            "messages": s.messages.to_string(),
            "bytes": s.bytes.to_string(),
          }
        }),
        "event_loop_ticks": probe_stop(),
      }
      println(report.stringify())
    } else {
      raise @mcap.io_error(
        "usage: async query INPUT TOPIC START END | record OUTPUT MODE | rotate PREFIX MODE",
      )
    }
  } catch {
    error => report_error("\{error}")
  }
}
