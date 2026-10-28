///|
pub(all) struct QualityRule {
  topic : String
  minimum_messages : Int
  max_gap : UInt64?
  check_sequence : Bool
  condition : String?
  warning : Bool
}

///|
pub fn quality_rule(topic : String) -> QualityRule {
  {
    topic,
    minimum_messages: 1,
    max_gap: None,
    check_sequence: false,
    condition: None,
    warning: false,
  }
}

///|
struct QualityState {
  rule : QualityRule
  predicate : JsonFilter?
  mut count : UInt64
  mut previous : UInt64?
  sequences : Map[Int, UInt]
  mut violations : UInt64
  details : Array[Json]
}

///|
fn QualityState::issue(
  self : QualityState,
  kind : String,
  start : UInt64,
  end : UInt64,
  maximum : Int,
) -> Unit {
  self.violations += 1UL
  if self.details.length() < maximum {
    self.details.push({
      "kind": kind,
      "start": start.to_string(),
      "end": end.to_string(),
    })
  }
}

///|
/// Evaluate the entire explicit closed interval, without query pagination.
/// Gap checks include leading/trailing silence. Sequence checks are per channel.
pub fn check_quality(
  source : QuerySource,
  rules : Array[QualityRule],
  start : UInt64,
  end : UInt64,
  detail_limit? : Int = 64,
  pending_bytes? : Int = 64 * 1024 * 1024,
  cancelled? : () -> Bool = fn() { false },
) -> Json raise McapError {
  if start > end ||
    rules.is_empty() ||
    rules.length() > 128 ||
    detail_limit < 0 ||
    detail_limit > 1024 ||
    pending_bytes < 1 {
    raise Invalid("invalid quality interval or budgets")
  }
  let names : Map[String, Bool] = {}
  let states = rules.map(fn(rule) {
    if rule.topic == "" ||
      rule.minimum_messages < 0 ||
      names.contains(rule.topic) {
      raise Invalid(
        "quality rules require unique nonempty topics and nonnegative counts",
      )
    }
    names[rule.topic] = true
    let predicate = rule.condition.map(fn(text) { json_filter(text) })
    QualityState::{
      rule,
      predicate,
      count: 0UL,
      previous: None,
      sequences: {},
      violations: 0UL,
      details: [],
    }
  })
  let channels : Map[Int, String] = {}
  for channel in source.declarations.channels {
    channels[channel.id] = channel.topic
  }
  let stream = (source.open)(
    {
      ..all_query(),
      topics: rules.map(fn(r) { r.topic }),
      start_time: start,
      end_time: end,
    },
    pending_bytes,
  )
  defer (stream.stop)()
  let mut last : UInt64? = None
  while (stream.next_message)() is Some(message) {
    if cancelled() {
      raise Invalid("quality check cancelled")
    }
    if message.log_time < start ||
      message.log_time > end ||
      (last is Some(t) && message.log_time < t) {
      raise Invalid("quality source violated ascending interval contract")
    }
    last = Some(message.log_time)
    guard channels.get(message.channel_id) is Some(topic) else {
      raise Invalid("quality message has undeclared channel")
    }
    for state in states {
      if state.rule.topic != topic {
        continue
      }
      if state.rule.max_gap is Some(gap) &&
        message.log_time - state.previous.unwrap_or(start) > gap {
        state.issue(
          "gap",
          state.previous.unwrap_or(start),
          message.log_time,
          detail_limit,
        )
      }
      if state.rule.check_sequence {
        if state.sequences.get(message.channel_id) is Some(previous) &&
          message.sequence != previous + 1U {
          state.issue(
            "sequence_discontinuity",
            message.log_time,
            message.log_time,
            detail_limit,
          )
        }
        state.sequences[message.channel_id] = message.sequence
      }
      if state.predicate is Some(predicate) && !predicate.matches(message.data) {
        state.issue(
          "condition",
          message.log_time,
          message.log_time,
          detail_limit,
        )
      }
      state.previous = Some(message.log_time)
      state.count += 1UL
    }
  }
  let mut failed = false
  let mut warned = false
  let reports : Array[Json] = states.map(fn(state) {
    if state.count < state.rule.minimum_messages.to_uint64() {
      state.issue("minimum_messages", start, end, detail_limit)
    }
    if state.rule.max_gap is Some(gap) &&
      end - state.previous.unwrap_or(start) > gap {
      state.issue("gap", state.previous.unwrap_or(start), end, detail_limit)
    }
    if state.violations > 0UL {
      if state.rule.warning {
        warned = true
      } else {
        failed = true
      }
    }
    {
      "topic": state.rule.topic,
      "messages": state.count.to_string(),
      "status": if state.violations == 0UL {
        "passed"
      } else if state.rule.warning {
        "warning"
      } else {
        "failed"
      },
      "violations": state.violations.to_string(),
      "details": state.details,
      "omitted_details": (state.violations - state.details.length().to_uint64()).to_string(),
    }
  })
  {
    "status": if failed {
      "failed"
    } else if warned {
      "warning"
    } else {
      "passed"
    },
    "start": start.to_string(),
    "end": end.to_string(),
    "rules": reports,
    "sequence_interpretation": "discontinuity is evidence, not proof of transport loss",
  }
}

///|
/// Match availability over two ascending streams, retaining the existing join's
/// bounded state. Basis points use integer arithmetic and include unmatched rows.
pub fn check_match_quality(
  left : MessageStream,
  right : MessageStream,
  tolerance : UInt64,
  minimum_basis_points : Int,
) -> Json raise McapError {
  if minimum_basis_points < 0 || minimum_basis_points > 10000 {
    (left.stop)()
    (right.stop)()
    raise Invalid("match threshold must be 0..10000 basis points")
  }
  let join = temporal_join(left, right, options={ ..join_options(), tolerance, })
  defer join.cancel()
  let mut total = 0UL
  let mut matched = 0UL
  while join.next() is Some(row) {
    total += 1UL
    if row.right is Some(_) {
      matched += 1UL
    }
  }
  let rate = if total == 0UL {
    0
  } else {
    (@bigint.BigInt::from_uint64(matched) *
    @bigint.BigInt::from_int(10000) /
    @bigint.BigInt::from_uint64(total)).to_int()
  }
  {
    "status": if total > 0UL && rate >= minimum_basis_points {
      "passed"
    } else {
      "failed"
    },
    "left_messages": total.to_string(),
    "matched_messages": matched.to_string(),
    "basis_points": rate,
    "minimum_basis_points": minimum_basis_points,
  }
}
