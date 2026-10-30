///|
fn tf_finite(value : Double) -> Bool {
  !value.is_inf() && !value.is_nan()
}

///|
pub(all) struct TfVector {
  x : Double
  y : Double
  z : Double
} derive(Eq, Debug)

///|
pub(all) struct TfQuaternion {
  x : Double
  y : Double
  z : Double
  w : Double
} derive(Eq, Debug)

///|
pub(all) struct TfTransform {
  translation : TfVector
  rotation : TfQuaternion
} derive(Eq, Debug)

///|
pub(all) struct TfStamped {
  parent : String
  child : String
  time : UInt64
  transform : TfTransform
  is_static : Bool
  origin : Message?
}

///|
struct TfEdge {
  parent : String
  is_static : Bool
  samples : Array[TfStamped]
}

///|
pub struct TfTree {
  edges : Map[String, TfEdge]
  sample_limit : Int32
  byte_limit : Int32
  mut count : Int32
  mut bytes : Int32
}

///|
pub(all) struct TfLookup {
  transform : TfTransform
  evidence : Array[TfStamped]
  interpolated : Bool
}

///|
pub fn tf_identity() -> TfTransform {
  {
    translation: { x: 0.0, y: 0.0, z: 0.0, },
    rotation: { x: 0.0, y: 0.0, z: 0.0, w: 1.0, },
  }
}

///|
fn tf_normalize(q : TfQuaternion) -> TfQuaternion raise McapError {
  let values = [q.x, q.y, q.z, q.w]
  if values.any(fn(v) { v.is_inf() || v.is_nan() }) {
    raise Invalid("nonfinite TF quaternion")
  }
  let scale = values.fold(init=0.0, fn(a, v) {
    if v.abs() > a {
      v.abs()
    } else {
      a
    }
  })
  if scale == 0.0 {
    raise Invalid("zero TF quaternion")
  }
  let x = q.x / scale
  let y = q.y / scale
  let z = q.z / scale
  let w = q.w / scale
  let norm = (x * x + y * y + z * z + w * w).sqrt()
  { x: x / norm, y: y / norm, z: z / norm, w: w / norm, }
}

///|
fn tf_valid(t : TfTransform) -> TfTransform raise McapError {
  if !tf_finite(t.translation.x) ||
    !tf_finite(t.translation.y) ||
    !tf_finite(t.translation.z) {
    raise Invalid("nonfinite TF translation")
  }
  { ..t, rotation: tf_normalize(t.rotation), }
}

///|
fn tf_rotate(q : TfQuaternion, v : TfVector) -> TfVector {
  let tx = 2.0 * (q.y * v.z - q.z * v.y)
  let ty = 2.0 * (q.z * v.x - q.x * v.z)
  let tz = 2.0 * (q.x * v.y - q.y * v.x)
  {
    x: v.x + q.w * tx + q.y * tz - q.z * ty,
    y: v.y + q.w * ty + q.z * tx - q.x * tz,
    z: v.z + q.w * tz + q.x * ty - q.y * tx,
  }
}

///|
pub fn tf_compose(
  a : TfTransform,
  b : TfTransform,
) -> TfTransform raise McapError {
  let a = tf_valid(a)
  let b = tf_valid(b)
  let p = tf_rotate(a.rotation, b.translation)
  let x = a.rotation
  let y = b.rotation
  tf_valid({
    translation: {
      x: a.translation.x + p.x,
      y: a.translation.y + p.y,
      z: a.translation.z + p.z,
    },
    rotation: {
      x: x.w * y.x + x.x * y.w + x.y * y.z - x.z * y.y,
      y: x.w * y.y - x.x * y.z + x.y * y.w + x.z * y.x,
      z: x.w * y.z + x.x * y.y - x.y * y.x + x.z * y.w,
      w: x.w * y.w - x.x * y.x - x.y * y.y - x.z * y.z,
    },
  })
}

///|
pub fn tf_inverse(t : TfTransform) -> TfTransform raise McapError {
  let t = tf_valid(t)
  let q = {
    x: 0.0 - t.rotation.x,
    y: 0.0 - t.rotation.y,
    z: 0.0 - t.rotation.z,
    w: t.rotation.w,
  }
  {
    rotation: q,
    translation: tf_rotate(q, {
      x: 0.0 - t.translation.x,
      y: 0.0 - t.translation.y,
      z: 0.0 - t.translation.z,
    }),
  }
}

///|
pub fn tf_apply(t : TfTransform, p : TfVector) -> TfVector raise McapError {
  let t = tf_valid(t)
  if !tf_finite(p.x) || !tf_finite(p.y) || !tf_finite(p.z) {
    raise Invalid("nonfinite TF point")
  }
  let r = tf_rotate(t.rotation, p)
  let out = {
    x: r.x + t.translation.x,
    y: r.y + t.translation.y,
    z: r.z + t.translation.z,
  }
  if !tf_finite(out.x) || !tf_finite(out.y) || !tf_finite(out.z) {
    raise Invalid("TF point overflow")
  }
  out
}

///|
pub fn tf_tree(
  sample_limit~ : Int32 = 1000000,
  byte_limit~ : Int32 = 128 * 1024 * 1024,
) -> TfTree raise McapError {
  if sample_limit < 1 || byte_limit < 1 {
    raise Invalid("invalid TF history budgets")
  }
  { edges: {}, sample_limit, byte_limit, count: 0, bytes: 0, }
}

///|
/// Each insertion is atomic. Parent changes and static/dynamic conflicts require
/// an explicitly separate tree/session instead of silently changing topology.
pub fn TfTree::add(self : TfTree, sample : TfStamped) -> Unit raise McapError {
  if sample.parent == "" ||
    sample.child == "" ||
    sample.parent == sample.child ||
    sample.parent.length() > 1024 ||
    sample.child.length() > 1024 {
    raise Invalid("invalid TF frame identifiers")
  }
  let sample = { ..sample, transform: tf_valid(sample.transform), }
  if self.edges.get(sample.child) is Some(edge) &&
    edge.parent == sample.parent &&
    edge.is_static == sample.is_static {
    let last = edge.samples[edge.samples.length() - 1]
    if (edge.is_static || last.time == sample.time) &&
      last.transform == sample.transform {
      return
    }
  }
  let base = 159 + sample.parent.length() * 4 + sample.child.length() * 4
  let payload = match sample.origin {
    Some(m) => m.data.length()
    None => 0
  }
  if base > self.byte_limit || payload > self.byte_limit - base {
    raise Invalid("TF history byte budget exceeded")
  }
  let cost = base + payload
  if self.count >= self.sample_limit || cost > self.byte_limit - self.bytes {
    raise Invalid("TF history budget exceeded")
  }
  match self.edges.get(sample.child) {
    Some(edge) => {
      if edge.parent != sample.parent ||
        edge.is_static != sample.is_static ||
        edge.is_static ||
        sample.time <= edge.samples[edge.samples.length() - 1].time {
        raise Invalid("TF parent/static conflict or nonincreasing timestamp")
      }
      edge.samples.push(sample)
    }
    None => {
      if self.edges.length() >= 4096 {
        raise Invalid("TF edge limit exceeded")
      }
      let mut frame = sample.parent
      while true {
        if frame == sample.child {
          raise Invalid("TF topology cycle")
        }
        match self.edges.get(frame) {
          Some(edge) => frame = edge.parent
          None => break
        }
      }
      self.edges[sample.child] = {
        parent: sample.parent,
        is_static: sample.is_static,
        samples: [sample],
      }
    }
  }
  self.count += 1
  self.bytes += cost
}

///|
fn tf_bracket(
  edge : TfEdge,
  time : UInt64,
  interpolate : Bool,
  max_gap : UInt64,
) -> (TfTransform, Array[TfStamped], Bool) raise McapError {
  let samples = edge.samples
  if edge.is_static {
    return (samples[0].transform, [samples[0]], false)
  }
  let mut lo = 0
  let mut hi = samples.length()
  while lo < hi {
    let mid = lo + (hi - lo) / 2
    if samples[mid].time < time {
      lo = mid + 1
    } else {
      hi = mid
    }
  }
  if lo < samples.length() && samples[lo].time == time {
    return (samples[lo].transform, [samples[lo]], false)
  }
  if lo == 0 || lo == samples.length() {
    raise Invalid("TF timestamp outside history; extrapolation disabled")
  }
  let a = samples[lo - 1]
  let b = samples[lo]
  if !interpolate || time - a.time > max_gap || b.time - time > max_gap {
    raise Invalid("TF interpolation disabled or gap exceeds tolerance")
  }
  let f = (time - a.time).to_double() / (b.time - a.time).to_double()
  let q = a.transform.rotation
  let raw = b.transform.rotation
  let dot = q.x * raw.x + q.y * raw.y + q.z * raw.z + q.w * raw.w
  let r = if dot < 0.0 {
    { x: 0.0 - raw.x, y: 0.0 - raw.y, z: 0.0 - raw.z, w: 0.0 - raw.w, }
  } else {
    raw
  }
  let dot = if dot < 0.0 { 0.0 - dot } else { dot }
  let (s, t) = if dot > 0.9995 {
    (1.0 - f, f)
  } else {
    let angle = @math.acos(if dot > 1.0 { 1.0 } else { dot })
    let denominator = @math.sin(angle)
    (
      @math.sin((1.0 - f) * angle) / denominator,
      @math.sin(f * angle) / denominator,
    )
  }
  let x = a.transform.translation
  let y = b.transform.translation
  let transform = tf_valid({
    translation: {
      x: (1.0 - f) * x.x + f * y.x,
      y: (1.0 - f) * x.y + f * y.y,
      z: (1.0 - f) * x.z + f * y.z,
    },
    rotation: {
      x: s * q.x + t * r.x,
      y: s * q.y + t * r.y,
      z: s * q.z + t * r.z,
      w: s * q.w + t * r.w,
    },
  })
  (transform, [a, b], true)
}

///|
fn TfTree::chain(
  self : TfTree,
  frame : String,
  time : UInt64,
  interpolate : Bool,
  max_gap : UInt64,
  stop? : String,
) -> (String, TfLookup) raise McapError {
  if frame == "" ||
    (
      !self.edges.contains(frame) &&
      !self.edges.values().any(fn(e) { e.parent == frame })
    ) {
    raise Invalid("TF frame unknown")
  }
  let mut node = frame
  let mut transform = tf_identity()
  let evidence = []
  let mut derived = false
  while self.edges.get(node) is Some(edge) {
    if stop == Some(node) {
      break
    }
    let (step, samples, was_interpolated) = tf_bracket(
      edge, time, interpolate, max_gap,
    )
    transform = tf_compose(step, transform)
    evidence.append(samples)
    derived = derived || was_interpolated
    node = edge.parent
  }
  (node, { transform, evidence, interpolated: derived, })
}

///|
/// Returns target_T_source. Dynamic edges never extrapolate.
pub fn TfTree::lookup(
  self : TfTree,
  target : String,
  source : String,
  time : UInt64,
  interpolate~ : Bool = true,
  max_gap~ : UInt64 = 18446744073709551615UL,
) -> TfLookup raise McapError {
  let ancestors : Map[String, Bool] = {}
  let mut frame = target
  ancestors[frame] = true
  while self.edges.get(frame) is Some(edge) {
    frame = edge.parent
    ancestors[frame] = true
  }
  let mut common = source
  while !ancestors.contains(common) {
    match self.edges.get(common) {
      Some(edge) => common = edge.parent
      None => raise Invalid("TF frames have disconnected roots")
    }
  }
  let (a, x) = self.chain(target, time, interpolate, max_gap, stop=common)
  let (b, y) = self.chain(source, time, interpolate, max_gap, stop=common)
  if a != b {
    raise Invalid("TF frames have disconnected roots")
  }
  {
    transform: tf_compose(tf_inverse(x.transform), y.transform),
    evidence: [..x.evidence, ..y.evidence],
    interpolated: x.interpolated || y.interpolated,
  }
}

///|
/// Retains static state and dynamic brackets plus all interior updates on the
/// ancestor paths of explicitly requested frames. Whole origin messages may
/// contain additional transforms; callers must not invent new source envelopes.
pub fn TfTree::context(
  self : TfTree,
  frames : Array[String],
  start : UInt64,
  end : UInt64,
) -> Array[TfStamped] raise McapError {
  if frames.is_empty() || frames.length() > 128 || start > end {
    raise Invalid("invalid TF context request")
  }
  let needed : Map[String, Bool] = {}
  for frame in frames {
    ignore(self.chain(frame, start, true, 18446744073709551615UL))
    ignore(self.chain(frame, end, true, 18446744073709551615UL))
    let mut node = frame
    while self.edges.get(node) is Some(edge) {
      needed[node] = true
      node = edge.parent
    }
  }
  let output = []
  let keys = needed.keys().to_array()
  keys.sort()
  for key in keys {
    let edge = self.edges[key]
    if edge.is_static {
      output.push(edge.samples[0])
      continue
    }
    for i = 0; i < edge.samples.length(); i = i + 1 {
      let sample = edge.samples[i]
      if (sample.time >= start && sample.time <= end) ||
        (
          sample.time < start &&
          i + 1 < edge.samples.length() &&
          edge.samples[i + 1].time > start
        ) ||
        (sample.time > end && i > 0 && edge.samples[i - 1].time < end) {
        output.push(sample)
      }
    }
  }
  output
}

///|
pub fn tf_transform_json(t : TfTransform) -> Json {
  {
    "translation": {
      "x": t.translation.x,
      "y": t.translation.y,
      "z": t.translation.z,
    },
    "rotation": {
      "x": t.rotation.x,
      "y": t.rotation.y,
      "z": t.rotation.z,
      "w": t.rotation.w,
    },
  }
}

///|
pub fn tf_stamped_json(s : TfStamped) -> Json {
  {
    "parent": s.parent,
    "child": s.child,
    "time": s.time.to_string(),
    "static": s.is_static,
    "transform": tf_transform_json(s.transform),
    "origin": match s.origin {
      None => Json::null()
      Some(m) =>
        (
          {
            "channel_id": m.channel_id,
            "sequence": m.sequence.to_string(),
            "log_time": m.log_time.to_string(),
            "publish_time": m.publish_time.to_string(),
          } : Json)
    },
  }
}

///|
pub fn parse_tf_message(
  value : Json,
  is_static : Bool,
  origin? : Message,
) -> Array[TfStamped] raise McapError {
  guard value is Object(body) &&
    body.get("transforms") is Some(Array(transforms)) &&
    transforms.length() <= 4096 else {
    raise Invalid("TF transforms array required (max 4096)")
  }
  fn member(value : Json, key : String) -> Json raise McapError {
    guard value is Object(fields) && fields.get(key) is Some(found) else {
      raise Invalid("missing TF member: " + key)
    }
    found
  }
  fn text(value : Json, key : String) -> String raise McapError {
    guard member(value, key) is String(s) else {
      raise Invalid("TF string required")
    }
    s
  }
  fn num(value : Json, key : String) -> Double raise McapError {
    guard member(value, key) is Number(n, ..) && tf_finite(n) else {
      raise Invalid("finite TF number required")
    }
    n
  }
  transforms.map(fn(item) {
    let header = member(item, "header")
    let stamp = member(header, "stamp")
    let stamp_message : Message = {
      channel_id: 0,
      sequence: 0U,
      log_time: 0UL,
      publish_time: 0UL,
      data: @utf8.encode(stamp.stringify()[:]),
    }
    let time = message_timestamp(
      stamp_message,
      SyncJsonStamp("/sec", "/nanosec"),
    )
    let transform = member(item, "transform")
    let p = member(transform, "translation")
    let q = member(transform, "rotation")
    {
      parent: text(header, "frame_id"),
      child: text(item, "child_frame_id"),
      time,
      is_static,
      origin,
      transform: tf_valid({
        translation: { x: num(p, "x"), y: num(p, "y"), z: num(p, "z"), },
        rotation: {
          x: num(q, "x"),
          y: num(q, "y"),
          z: num(q, "z"),
          w: num(q, "w"),
        },
      }),
    }
  })
}
