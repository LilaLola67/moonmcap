///|
pub(all) struct ClockAnchor {
  source : UInt64
  target : UInt64
}

///|
pub struct ClockMap {
  anchors : Array[ClockAnchor]
  extrapolate : Bool
}

///|
/// Monotone, piecewise affine map. Integer interpolation rounds down. Anchors
/// are supplied by the caller, never inferred from unrelated sensor values.
pub fn clock_map(
  anchors : Array[ClockAnchor],
  extrapolate~ : Bool = false,
) -> ClockMap raise McapError {
  if anchors.length() < 2 || anchors.length() > 4096 {
    raise Invalid("clock map needs 2..4096 anchors")
  }
  for i = 1; i < anchors.length(); i = i + 1 {
    if anchors[i].source <= anchors[i - 1].source ||
      anchors[i].target <= anchors[i - 1].target {
      raise Invalid("clock anchors must strictly increase on both clocks")
    }
  }
  { anchors: anchors.copy(), extrapolate, }
}

///|
pub fn ClockMap::map(self : ClockMap, time : UInt64) -> UInt64 raise McapError {
  let last = self.anchors.length() - 1
  if !self.extrapolate &&
    (time < self.anchors[0].source || time > self.anchors[last].source) {
    raise Invalid("timestamp outside calibrated clock domain")
  }
  let mut lo = 0
  let mut hi = last
  while hi - lo > 1 {
    let middle = lo + (hi - lo) / 2
    if self.anchors[middle].source <= time {
      lo = middle
    } else {
      hi = middle
    }
  }
  let a = self.anchors[lo]
  let b = self.anchors[lo + 1]
  let delta = @bigint.BigInt::from_uint64(time) -
    @bigint.BigInt::from_uint64(a.source)
  let numerator = delta * @bigint.BigInt::from_uint64(b.target - a.target)
  let denominator = @bigint.BigInt::from_uint64(b.source - a.source)
  let mut offset = numerator / denominator
  // BigInt division truncates toward zero. Use mathematical floor for negatives.
  if numerator < @bigint.BigInt::from_int(0) &&
    numerator % denominator != @bigint.BigInt::from_int(0) {
    offset -= @bigint.BigInt::from_int(1)
  }
  let value = @bigint.BigInt::from_uint64(a.target) + offset
  if value < @bigint.BigInt::from_int(0) ||
    value > @bigint.BigInt::from_uint64(18446744073709551615UL) {
    raise Invalid("calibrated timestamp overflow or underflow")
  }
  value.to_uint64()
}

///|
pub fn ClockMap::json(self : ClockMap) -> Json {
  {
    "rounding": "floor",
    "extrapolate": self.extrapolate,
    "anchors": self.anchors.map(fn(a) {
      let value : Json = {
        "source": a.source.to_string(),
        "target": a.target.to_string(),
      }
      value
    }),
  }
}

///|
/// Independent held-out probes; anchor interpolation residual alone is not
/// evidence of calibration accuracy. Signed residual is predicted minus target.
pub fn ClockMap::residuals(
  self : ClockMap,
  probes : Array[ClockAnchor],
) -> Json raise McapError {
  if probes.length() > 10000 {
    raise Invalid("too many clock probes")
  }
  let rows : Array[Json] = probes.map(fn(probe) {
    let predicted = self.map(probe.source)
    let residual = @bigint.BigInt::from_uint64(predicted) -
      @bigint.BigInt::from_uint64(probe.target)
    {
      "source": probe.source.to_string(),
      "target": probe.target.to_string(),
      "predicted": predicted.to_string(),
      "residual_ns": residual.to_string(),
    }
  })
  { "probes": rows, "sign": "predicted minus reference target" }
}

///|
/// Query times and pagination are in corrected log-time. Scan the original
/// calibrated domain and filter exactly, avoiding unsafe inverse rounding.
pub fn QuerySource::calibrate_time(
  self : QuerySource,
  mapping : ClockMap,
  same_publish_clock~ : Bool = false,
) -> QuerySource raise McapError {
  query_source(
    self.declarations,
    fn(query, budget) {
      if query.start_time > query.end_time ||
        query.offset < 0 ||
        query.limit < 0 {
        raise Invalid("invalid corrected-time query")
      }
      let inner = (self.open)(
        {
          ..query,
          start_time: if mapping.extrapolate {
            0UL
          } else {
            mapping.anchors[0].source
          },
          end_time: if mapping.extrapolate {
            18446744073709551615UL
          } else {
            mapping.anchors[mapping.anchors.length() - 1].source
          },
          offset: 0,
          limit: 2147483647,
        },
        budget,
      )
      let skipped = [query.offset]
      let remaining = [query.limit]
      let closed = [false]
      fn stop() -> Unit {
        if !closed[0] {
          closed[0] = true
          (inner.stop)()
        }
      }
      message_stream(
        fn() {
          if closed[0] || remaining[0] == 0 {
            stop()
            return None
          }
          try {
            while (inner.next_message)() is Some(message) {
              let log_time = mapping.map(message.log_time)
              if log_time < query.start_time || log_time > query.end_time {
                continue
              }
              if skipped[0] > 0 {
                skipped[0] -= 1
                continue
              }
              let publish_time = if same_publish_clock {
                mapping.map(message.publish_time)
              } else {
                message.publish_time
              }
              remaining[0] -= 1
              if remaining[0] == 0 {
                stop()
              }
              return Some({ ..message, log_time, publish_time, })
            }
            stop()
            None
          } catch {
            error => {
              stop()
              raise error
            }
          }
        },
        stop,
        pending_bytes=inner.pending,
      )
    },
    materialized=self.materialized,
  )
}

///|
/// Rewrite all log clocks, optionally publish/create clocks only when the caller
/// asserts the same clock domain. Payload timestamps are deliberately untouched.
pub fn calibrate_log(
  log : Log,
  mapping : ClockMap,
  same_publish_clock~ : Bool = false,
) -> Log raise McapError {
  validate_log(log)
  let messages = build_query_index(log)
    .query(all_query())
    .map(fn(m) {
      {
        ..m,
        log_time: mapping.map(m.log_time),
        publish_time: if same_publish_clock {
          mapping.map(m.publish_time)
        } else {
          m.publish_time
        },
      }
    })
  let attachments = log.attachments.map(fn(a) {
    {
      ..a,
      log_time: mapping.map(a.log_time),
      create_time: if same_publish_clock {
        mapping.map(a.create_time)
      } else {
        a.create_time
      },
    }
  })
  let manifest : Json = {
    "mapping": mapping.json(),
    "same_publish_clock": same_publish_clock,
    "payload_timestamps": "unchanged",
    "indexes": "must be rebuilt by writer",
  }
  {
    ..copy_log(log),
    messages,
    attachments,
    metadata: log.metadata +
    [
      {
        name: "moonmcap.clock-map",
        entries: [("manifest", manifest.stringify())],
      },
    ],
  }
}
