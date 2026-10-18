# Third-party code and licenses

## moonbitlang/async 0.20.2 — Apache-2.0

Source: https://github.com/moonbitlang/async
Published module: https://mooncakes.io/docs/moonbitlang/async

The optional `asyncio` package uses the official coroutine runtime and JavaScript
Promise bridge through a pinned dependency. No async runtime code is copied into
the container core. Node file/network adapters, queues and MCAP operations are
maintained in this repository. The dependency retains its upstream Apache-2.0 license.

The MCAP container, framing, CRC32, indexing, query, transformation and recovery
implementation is maintained in the root package under MIT.

## bikallem/compress 0.3.4 — Apache-2.0

Source: https://github.com/bikallem/compress
Published module: https://mooncakes.io/docs/bikallem/compress

- The module dependency supplies the LZ4 frame encoder and xxHash checksums.
- `internal/zstd/*.mbt` is derived from the `zstd` package of the published
  version 0.3.4, not an original MCAP codec or a new compression algorithm.
- Local changes add bounded block expansion and cumulative output checks,
  enlarge the adapter's permitted window to 64 MiB after preflight validation,
  and encode independent, explicitly offset 128 KiB blocks to keep noisy and large inputs within the format limits.
- The upstream encoder accepts a level argument but does not implement distinct
  level strategies. MoonMCAP therefore offers a fixed Zstd encoding strategy,
  without claiming a configurable level or equivalence to libzstd level 3.

The copyright in this implementation remains with its original contributors.
The license is reproduced in [Apache-2.0.txt](Apache-2.0.txt). Source files retain
the upstream implementation, apart from the documented adapter changes.
Third-party sources are excluded from `tools/count_source.py`.

## bikallem/blit 0.2.2 — Apache-2.0

Source: https://github.com/bikallem/blit
Used as a module dependency for the vendored Zstd buffers. Its source is not
copied into the root implementation or counted as original project code.

## Official Python MCAP reference — MIT

Source: https://github.com/foxglove/mcap
`mcap==1.3.0` is used only to generate synthetic fixtures and validate
interoperability. Python and its LZ4/Zstandard wheels are development tools,
not runtime dependencies of the MoonBit library. Generated fixture tests are
identified as such and excluded from implementation line counts.
