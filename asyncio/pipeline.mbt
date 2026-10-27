///|
pub struct AsyncQueryPlan {
  provider : AsyncQuerySource
  source : AsyncSource
  index : AsyncSource?
  fields : AsyncSource?
  backend : String
  verified : Bool
  opening_bytes : UInt64
  decisions : Map[UInt64, Bool]
  statistics : () -> Json
  paged_fields : Array[AsyncPagedFields]
}

///|
/// The source/index handles and optional pool remain owned by the caller.
/// Field pruning is followed by exact decoding/filtering before result paging.
pub async fn plan_query(
  source : AsyncSource,
  index? : AsyncSource,
  fields? : AsyncSource,
  pool? : WorkerPool,
  filter? : @mcap.JsonFilter,
  decode? : Bool = false,
  cache_bytes? : Int = 8 * 1024 * 1024,
  fallback_bytes? : Int = 64 * 1024 * 1024,
  max_decoded_bytes? : Int = 1024 * 1024,
) -> AsyncQueryPlan {
  if fields is Some(_) && filter is None {
    raise @mcap.io_error("field pruning requires an exact filter")
  }
  let digest = if fields is Some(_) || index is Some(_) {
    source_sha256(source)
  } else {
    ""
  }
  let decisions : Map[UInt64, Bool] = Map([])
  let paged_fields : Array[AsyncPagedFields] = []
  let raw_select = match fields {
    None => async fn(_, _) { true }
    Some(fields) => {
      if fields.size() < 16UL {
        raise @mcap.io_error("short field index")
      }
      if fields.read_exact(0UL, 8) == b"MFLD2\r\n\x00" {
        let field = open_paged_fields(
          fields,
          source.size(),
          digest,
          "moonmcap.std.json-protobuf-cdr.numeric.v1",
        )
        paged_fields.push(field)
        async fn(offset, length) {
          field.may_match(offset, length, filter.unwrap())
        }
      } else {
        if fields.size() > 16UL * 1024UL * 1024UL {
          raise @mcap.io_error("MFLD1 exceeds bounded field index budget")
        }
        let mirror = @mcap.new_read_source(source.size(), fn(_, _) {
          raise @mcap.io_error("unexpected field fingerprint source read")
        })
        let field = @mcap.open_field_index(
          mirror,
          fields.read_exact(0UL, fields.size().to_int()),
          fn(_) { digest },
          "moonmcap.std.json-protobuf-cdr.numeric.v1",
        )
        let select = field.location_filter(filter.unwrap())
        async fn(offset, length) { select(offset, length) }
      }
    }
  }
  let select_region = async fn(offset : UInt64, length : Int) {
    let keep = raw_select(offset, length)
    // Explain state is bounded; it does not participate in correctness.
    if decisions.length() < 10000 || decisions.contains(offset) {
      decisions[offset] = keep
    }
    keep
  }
  let (provider, backend, statistics) = match index {
    Some(index) => {
      let reader = open_disk_sidecar(
        source,
        index,
        AsyncVerifyContent(async fn(_) { digest }),
        pool?,
        select_region~,
        chunk_cache_bytes=cache_bytes,
      )
      (reader.query_source(), "MIDX2", fn() { reader.info() })
    }
    None =>
      match pool {
        Some(pool) => {
          let reader = open_parallel_reader(
            source,
            pool,
            select_region~,
            cache_bytes~,
            fallback_bytes~,
          )
          (reader.query_source(), "parallel-native", fn() { reader.info() })
        }
        None => {
          let reader = open_reader(source, cache_bytes~, fallback_bytes~)
          (
            reader.query_source(select_region~),
            "native",
            fn() -> Json {
              {
                "decoded_chunks": reader.reader.diagnostics().decoded_chunks,
                "indexed": reader.reader.diagnostics().indexed,
              }
            },
          )
        }
      }
  }
  let provider = if decode || filter is Some(_) {
    match pool {
      None =>
        provider.decode_payload(
          @mcap.standard_payload_decoder(),
          max_bytes=max_decoded_bytes,
        )
      Some(pool) => provider.decode_parallel(pool, max_bytes=max_decoded_bytes)
    }
  } else {
    provider
  }
  let provider = match filter {
    None => provider
    Some(filter) => provider.filter_json(filter)
  }
  {
    provider,
    source,
    index,
    fields,
    backend,
    verified: digest != "",
    opening_bytes: source.metrics().bytes_read,
    decisions,
    statistics,
    paged_fields,
  }
}

///|
pub fn AsyncQueryPlan::query_source(self : AsyncQueryPlan) -> AsyncQuerySource {
  self.provider
}

///|
pub fn AsyncQueryPlan::info(self : AsyncQueryPlan) -> Json {
  {
    "backend": self.backend,
    "content_verified": self.verified,
    "opening_source_bytes": self.opening_bytes.to_string(),
    "query_source_bytes": (self.source.metrics().bytes_read - self.opening_bytes).to_string(),
    "index_bytes_read": self.index
    .map(fn(s) { s.metrics().bytes_read.to_string() })
    .unwrap_or("0"),
    "field_bytes_read": self.fields
    .map(fn(s) { s.metrics().bytes_read.to_string() })
    .unwrap_or("0"),
    "tracked_regions": self.decisions.length(),
    "skipped_regions": self.decisions
    .values()
    .filter(fn(keep) { !keep })
    .count(),
    "region_tracking_limit": 10000,
    "reader": (self.statistics)(),
    "paged_fields": Json::array(self.paged_fields.map(fn(f) { f.info() })),
  }
}
