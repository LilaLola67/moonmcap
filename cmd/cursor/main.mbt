///|
fn print_page(label : String, messages : Array[@mcap.Message]) -> Unit {
  let sequences = messages.map(fn(message) { message.sequence })
  println("\{label}: sequence=\{sequences}")
}

///|
fn main {
  try {
    let recorder = @mcap.new_builder()
    let imu = recorder.register_channel("/imu", "raw")
    let camera = recorder.register_channel("/camera", "raw")
    // Out-of-order times and ties illustrate stable ordering across pages.
    for time in [30UL, 10UL, 20UL, 20UL, 40UL, 0UL] {
      recorder.append(imu, time, b"imu")
      recorder.append(camera, time, b"camera")
    }
    let bytes = recorder.finish(compression=@mcap.Zstd, messages_per_chunk=3)
    let index = @mcap.build_query_index(@mcap.read_log(bytes))
    let cursor = index.cursor({ ..@mcap.all_query(), topics: ["/imu"], })
    print_page("page 1", cursor.next_page(2))
    print_page("page 2", cursor.next_page(2))
    cursor.seek(20UL)
    print_page("seek 20 inclusive", cursor.next_page(3))
    let progress = cursor.progress()
    println(
      "after seek: matched=\{progress.matching_messages}, emitted=\{progress.emitted_messages}, remaining=\{progress.remaining_messages}",
    )
    cursor.cancel()
    if cursor.next() is Some(_) {
      abort("cancelled cursor returned a message")
    }
    let reverse = index.cursor({
      ..@mcap.all_query(),
      topics: ["/imu"],
      descending: true,
      limit: 3,
    })
    print_page("reverse", reverse.next_page(10))
    println("cursor example: PASS")
  } catch {
    @mcap.Invalid(reason) => abort("cursor example failed: \{reason}")
  }
}
