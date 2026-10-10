///|
/// An in-memory time index, not an on-disk MCAP MessageIndex.
pub struct QueryIndex {
  log : Log
  by_channel : Map[Int, Array[Int]]
  all_messages : Array[Int]
}

///|
pub(all) struct Query {
  topics : Array[String]
  start_time : UInt64
  end_time : UInt64
  descending : Bool
  offset : Int
  limit : Int
}

///|
pub fn all_query() -> Query {
  {topics: [], start_time: 0UL, end_time: 18446744073709551615UL, descending: false, offset: 0, limit: 100000}
}

///|
fn sort_positions(log : Log, positions : Array[Int]) -> Unit {
  positions.sort_by(fn(a, b) {
    let ta = log.messages[a].log_time
    let tb = log.messages[b].log_time
    if ta < tb { -1 } else if ta > tb { 1 } else { a.compare(b) }
  })
}

///|
/// Copy mutable input bytes and arrays so later caller mutations do not stale the index.
pub fn copy_log(log : Log) -> Log {
  {
    header: log.header,
    schemas: log.schemas.map(fn(s) { { ..s, data: s.data.copy() } }),
    channels: log.channels.map(fn(c) { { ..c, metadata: c.metadata.copy() } }),
    messages: log.messages.map(fn(m) { { ..m, data: m.data.copy() } }),
    metadata: log.metadata.map(fn(m) { { ..m, entries: m.entries.copy() } }),
  }
}

///|
pub fn build_query_index(log : Log) -> QueryIndex raise McapError {
  validate_log(log)
  let log = copy_log(log)
  let by_channel : Map[Int, Array[Int]] = {}
  let all_messages = []
  for i = 0; i < log.messages.length(); i = i + 1 {
    let id = log.messages[i].channel_id
    match by_channel.get(id) {
      Some(positions) => positions.push(i)
      None => by_channel[id] = [i]
    }
    all_messages.push(i)
  }
  for positions in by_channel.values() { sort_positions(log, positions) }
  sort_positions(log, all_messages)
  { log, by_channel, all_messages }
}

///|
fn lower_bound(log : Log, positions : Array[Int], time : UInt64, exclusive : Bool) -> Int {
  let mut left = 0
  let mut right = positions.length()
  while left < right {
    let middle = left + (right - left) / 2
    let current = log.messages[positions[middle]].log_time
    if current < time || (exclusive && current == time) {
      left = middle + 1
    } else { right = middle }
  }
  left
}

///|
pub fn QueryIndex::query(self : QueryIndex, query : Query) -> Array[Message] raise McapError {
  if query.start_time > query.end_time || query.offset < 0 || query.limit < 0 {
    raise Invalid("invalid query range or pagination")
  }
  if query.limit == 0 { return [] }
  let candidates = []
  if query.topics.is_empty() {
    let start = lower_bound(self.log, self.all_messages, query.start_time, false)
    let end = lower_bound(self.log, self.all_messages, query.end_time, true)
    for i = start; i < end; i = i + 1 { candidates.push(self.all_messages[i]) }
  } else {
    for channel in self.log.channels {
      if query.topics.contains(channel.topic) {
        match self.by_channel.get(channel.id) {
          None => ()
          Some(positions) => {
            let start = lower_bound(self.log, positions, query.start_time, false)
            let end = lower_bound(self.log, positions, query.end_time, true)
            for i = start; i < end; i = i + 1 { candidates.push(positions[i]) }
          }
        }
      }
    }
    sort_positions(self.log, candidates)
  }
  let result = []
  let begin = query.offset.min(candidates.length())
  let count = query.limit.min(candidates.length() - begin)
  for i = begin; i < begin + count; i = i + 1 {
    let position = if query.descending { candidates[candidates.length() - 1 - i] } else { candidates[i] }
    let message = self.log.messages[position]
    result.push({ ..message, data: message.data.copy() })
  }
  result
}
