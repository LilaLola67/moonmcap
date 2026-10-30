///|
/// Clock is monotonic nanoseconds; wait and emit must apply backpressure.
/// Sleeps are capped at 50 ms so cancellation and controls can be observed.
pub async fn playback(
  replay : @mcap.Replay,
  clock : () -> UInt64,
  wait : async (UInt64) -> Unit,
  emit : async (@mcap.Message) -> Unit,
  cancelled~ : () -> Bool = fn() { false },
) -> Unit {
  try {
    while true {
      if cancelled() {
        replay.cancel()
        return
      }
      match replay.poll(clock()) {
        @mcap.ReplayEnd => return
        @mcap.ReplayIdle => wait(10000000UL)
        @mcap.ReplayWait(delay) =>
          wait(if delay > 50000000UL { 50000000UL } else { delay })
        @mcap.ReplayEvent(message) => emit(message)
      }
    }
  } catch {
    error => {
      replay.cancel()
      raise error
    }
  }
}
