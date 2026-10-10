///|
/// Stateful recorder. Failed additions are atomic and finishing freezes the builder.
pub struct LogBuilder {
  log : Log
  limits : Limits
  mut closed : Bool
  mut next_schema : Int
  mut next_channel : Int
  sequences : Map[Int, UInt]
}

///|
pub fn new_builder(
  profile? : String = "",
  limits? : Limits = default_limits(),
) -> LogBuilder raise McapError {
  let log = empty_log(profile~)
  validate_log(log, limits~)
  {
    log,
    limits,
    closed: false,
    next_schema: 1,
    next_channel: 1,
    sequences: {},
  }
}

///|
fn LogBuilder::require_open(self : LogBuilder) -> Unit raise McapError {
  if self.closed {
    raise Invalid("recorder already finished")
  }
}

///|
pub fn LogBuilder::register_schema(
  self : LogBuilder,
  name : String,
  encoding : String,
  data : Bytes,
) -> Int raise McapError {
  self.require_open()
  for schema in self.log.schemas {
    if schema.name == name && schema.encoding == encoding && schema.data == data {
      return schema.id
    }
  }
  if self.next_schema > 65535 {
    raise Invalid("schema identifier space exhausted")
  }
  let schema : Schema = {
    id: self.next_schema,
    name,
    encoding,
    data: data.copy(),
  }
  let candidate = copy_log(self.log)
  candidate.schemas.push(schema)
  validate_log(candidate, limits=self.limits)
  self.log.schemas.push(schema)
  self.next_schema += 1
  schema.id
}

///|
pub fn LogBuilder::register_channel(
  self : LogBuilder,
  topic : String,
  message_encoding : String,
  schema_id? : Int = 0,
  metadata? : Array[(String, String)] = [],
) -> Int raise McapError {
  self.require_open()
  for channel in self.log.channels {
    if channel.topic == topic &&
      channel.schema_id == schema_id &&
      channel.message_encoding == message_encoding &&
      channel.metadata == metadata {
      return channel.id
    }
  }
  if self.next_channel > 65535 {
    raise Invalid("channel identifier space exhausted")
  }
  let channel : Channel = {
    id: self.next_channel,
    schema_id,
    topic,
    message_encoding,
    metadata: metadata.copy(),
  }
  let candidate = copy_log(self.log)
  candidate.channels.push(channel)
  validate_log(candidate, limits=self.limits)
  self.log.channels.push(channel)
  self.sequences[channel.id] = 0U
  self.next_channel += 1
  channel.id
}

///|
pub fn LogBuilder::append(
  self : LogBuilder,
  channel_id : Int,
  log_time : UInt64,
  data : Bytes,
  publish_time? : UInt64 = log_time,
) -> Unit raise McapError {
  self.require_open()
  let sequence = match self.sequences.get(channel_id) {
    None => raise Invalid("recorder channel is not registered")
    Some(value) => value
  }
  let message : Message = {
    channel_id,
    sequence,
    log_time,
    publish_time,
    data: data.copy(),
  }
  let candidate = copy_log(self.log)
  candidate.messages.push(message)
  validate_log(candidate, limits=self.limits)
  self.log.messages.push(message)
  self.sequences[channel_id] = if sequence == 4294967295U {
    0U
  } else {
    sequence + 1U
  }
}

///|
pub fn LogBuilder::add_metadata(
  self : LogBuilder,
  value : Metadata,
) -> Unit raise McapError {
  self.require_open()
  let copied = { ..value, entries: value.entries.copy(), }
  let candidate = copy_log(self.log)
  candidate.metadata.push(copied)
  validate_log(candidate, limits=self.limits)
  self.log.metadata.push(copied)
}

///|
pub fn LogBuilder::add_attachment(
  self : LogBuilder,
  value : Attachment,
) -> Unit raise McapError {
  self.require_open()
  let copied = { ..value, data: value.data.copy(), }
  let candidate = copy_log(self.log)
  candidate.attachments.push(copied)
  validate_log(candidate, limits=self.limits)
  self.log.attachments.push(copied)
}

///|
pub fn LogBuilder::snapshot(self : LogBuilder) -> Log {
  copy_log(self.log)
}

///|
pub fn LogBuilder::finish(
  self : LogBuilder,
  indexed? : Bool = true,
  messages_per_chunk? : Int = 256,
) -> Bytes raise McapError {
  self.require_open()
  let bytes = if indexed {
    write_indexed(self.log, messages_per_chunk~, limits=self.limits)
  } else {
    write_log(self.log, limits=self.limits)
  }
  self.closed = true
  bytes
}

///|
pub fn LogBuilder::message_count(self : LogBuilder) -> Int {
  self.log.messages.length()
}
