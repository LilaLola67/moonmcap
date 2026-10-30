///|
pub struct AsyncPagedFields {
  index : AsyncSource
  header : @mcap.PagedFieldHeader
  pages : Array[(Int32, @mcap.FieldPage)]
  cache_limit : Int32
  mut reads : Int32
  mut hits : Int32
  mut peak_pages : Int32
}

///|
/// Audits every page with constant retained state before allowing pruning.
pub async fn open_paged_fields(
  index : AsyncSource,
  source_size : UInt64,
  digest : String,
  decoder_identity : String,
  cache_pages~ : Int32 = 4,
) -> AsyncPagedFields {
  if index.size() < 16UL || cache_pages < 0 || cache_pages > 64 {
    raise @mcap.io_error("invalid paged field envelope or cache budget")
  }
  let prefix = index.read_exact(0UL, 12)
  let mut length = 0UL
  for i = 0; i < 4; i = i + 1 {
    length = length | (prefix[8 + i].to_uint64() << (8 * i))
  }
  if length > 4080UL || length + 16UL > index.size() {
    raise @mcap.io_error("paged field header exceeds budget")
  }
  let header = @mcap.open_paged_field_header(
    source_size,
    index.size(),
    prefix + index.read_exact(12UL, length.to_int() + 4),
    digest,
    decoder_identity,
  )
  let reader : AsyncPagedFields = {
    index,
    header,
    pages: [],
    cache_limit: cache_pages,
    reads: 0,
    hits: 0,
    peak_pages: 0,
  }
  let mut previous = 0UL
  for n = 0; n < header.layout().pages; n = n + 1 {
    let page = reader.load(n)
    if page.first_offset() < previous {
      raise @mcap.io_error("field pages overlap or are out of order")
    }
    previous = page.end_offset()
  }
  reader
}

///|
async fn AsyncPagedFields::load(
  self : AsyncPagedFields,
  number : Int32,
) -> @mcap.FieldPage {
  for i = 0; i < self.pages.length(); i = i + 1 {
    if self.pages[i].0 == number {
      let page = self.pages.remove(i)
      self.pages.push(page)
      self.hits += 1
      return page.1
    }
  }
  let layout = self.header.layout()
  if number < 0 || number >= layout.pages {
    raise @mcap.io_error("field page number out of range")
  }
  let bytes = self.index.read_exact(
    layout.header_bytes.to_uint64() +
    number.to_uint64() * layout.page_bytes.to_uint64(),
    layout.page_bytes,
  )
  let page = self.header.decode_page(bytes)
  self.reads += 1
  if self.cache_limit > 0 {
    // Another task may have filled the same slot while this IO was in flight.
    self.pages.retain(fn(item) { item.0 != number })
    while self.pages.length() >= self.cache_limit {
      ignore(self.pages.remove(0))
    }
    self.pages.push((number, page))
    self.peak_pages = self.peak_pages.max(self.pages.length())
  }
  page
}

///|
pub async fn AsyncPagedFields::may_match(
  self : AsyncPagedFields,
  offset : UInt64,
  span : Int32,
  filter : @mcap.JsonFilter,
) -> Bool {
  let mut low = 0
  let mut high = self.header.layout().pages
  while low < high {
    let middle = low + (high - low) / 2
    let page = self.load(middle)
    if offset < page.first_offset() {
      high = middle
    } else if offset >= page.end_offset() {
      low = middle + 1
    } else {
      return page.may_match(offset, span, filter)
    }
  }
  true // Regions without trustworthy summaries must remain candidates.
}

///|
pub fn AsyncPagedFields::info(self : AsyncPagedFields) -> Json {
  {
    "pages": self.header.layout().pages,
    "page_bytes": self.header.layout().page_bytes,
    "cache_limit": self.cache_limit,
    "cached_pages": self.pages.length(),
    "peak_cached_pages": self.peak_pages,
    "page_reads": self.reads,
    "cache_hits": self.hits,
    "index_bytes_read": self.index.metrics().bytes_read.to_string(),
  }
}
