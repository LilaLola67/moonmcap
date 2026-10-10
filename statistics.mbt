///|
pub(all) struct TopicStats {
  channel_id : Int
  topic : String
  message_count : Int
  payload_bytes : UInt64
  first_time : UInt64?
  last_time : UInt64?
  maximum_gap : UInt64
  median_gap : UInt64
  p95_gap : UInt64
  arrival_regressions : Int
  sequence_discontinuities : Int
  publish_after_log : Int
  frequency_hz : Double
}

///|
pub(all) struct LogStats {
  schema_count : Int
  channel_count : Int
  message_count : Int
  payload_bytes : UInt64
  topics : Array[TopicStats]
}

///|
pub fn summarize(log : Log) -> LogStats raise McapError {
  validate_log(log)
  let topics = []
  let mut payload_bytes = 0UL
  for channel in log.channels {
    let messages = log.messages.filter(fn(message) { message.channel_id == channel.id })
    let times : Array[UInt64] = []
    let mut bytes = 0UL
    let mut arrival_regressions = 0
    let mut sequence_discontinuities = 0
    let mut publish_after_log = 0
    let mut previous_time : UInt64? = None
    let mut previous_sequence : UInt? = None
    for message in messages {
      times.push(message.log_time)
      bytes += message.data.length().to_uint64()
      match previous_time {
        Some(previous) => if message.log_time < previous { arrival_regressions += 1 }
        None => ()
      }
      match previous_sequence {
        Some(previous) => if previous != 0U && message.sequence != 0U && message.sequence != previous + 1U {
          sequence_discontinuities += 1
        }
        None => ()
      }
      if message.publish_time > message.log_time { publish_after_log += 1 }
      previous_time = Some(message.log_time)
      previous_sequence = Some(message.sequence)
    }
    times.sort()
    let gaps : Array[UInt64] = []
    for i = 1; i < times.length(); i = i + 1 { gaps.push(times[i] - times[i-1]) }
    gaps.sort()
    let first_time = if times.is_empty() { None } else { Some(times[0]) }
    let last_time = if times.is_empty() { None } else { Some(times[times.length()-1]) }
    let maximum_gap = if gaps.is_empty() { 0UL } else { gaps[gaps.length()-1] }
    let median_gap = if gaps.is_empty() { 0UL } else { gaps[(gaps.length()-1)/2] }
    let p95_gap = if gaps.is_empty() { 0UL } else { gaps[(gaps.length()-1)*95/100] }
    let frequency_hz = if times.length() > 1 && times[times.length()-1] > times[0] {
      (times.length()-1).to_double() * 1000000000.0 / (times[times.length()-1] - times[0]).to_double()
    } else { 0.0 }
    topics.push({channel_id: channel.id, topic: channel.topic, message_count: messages.length(), payload_bytes: bytes,
      first_time, last_time, maximum_gap, median_gap, p95_gap, arrival_regressions,
      sequence_discontinuities, publish_after_log, frequency_hz})
    payload_bytes += bytes
  }
  {schema_count: log.schemas.length(), channel_count: log.channels.length(), message_count: log.messages.length(), payload_bytes, topics}
}

///|
/// Human-readable report without converting timestamps to floating-point numbers.
pub fn stats_report(stats : LogStats) -> String {
  let out = StringBuilder::new()
  out.write_string("MoonMCAP log summary\n")
  out.write_string("schemas=\{stats.schema_count} channels=\{stats.channel_count} messages=\{stats.message_count} payload_bytes=\{stats.payload_bytes}\n")
  for topic in stats.topics {
    out.write_string("topic=\{topic.topic} channel=\{topic.channel_id} count=\{topic.message_count} bytes=\{topic.payload_bytes}\n")
    match (topic.first_time, topic.last_time) {
      (Some(first), Some(last)) => out.write_string("  time_ns=[\{first},\{last}] maximum_gap_ns=\{topic.maximum_gap}\n")
      _ => out.write_string("  no messages\n")
    }
    out.write_string("  arrival_regressions=\{topic.arrival_regressions} sequence_discontinuities=\{topic.sequence_discontinuities} publish_after_log=\{topic.publish_after_log}\n")
  }
  out.to_string()
}
