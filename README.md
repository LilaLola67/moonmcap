# MoonMCAP

MoonBit-native MCAP message-log library. Application-stage implementation.

## Goal

Build a reusable binary codec, bounded reader/writer, indexed topic/time queries
and semantic log transformations. Payload bytes are opaque; this is not a ROS
message decoder or a browser SQLite bag inspector.

## Toolchain

MoonBit compiler **0.10.14 or later**. Development: v0.10.14+7d59c7ec9.
Only MoonBit core dependencies. Run `moon check`, `moon test`, `moon build`.

## Status

- Implemented: typed models and configurable resource budgets.
- Planned: codecs, CRC, readers/writers, chunk indexes, queries and transforms.
- Acceptance target: at least 2,500 handwritten implementation lines, excluding
  tests, generated interfaces, examples, comments, blank lines and dependencies.
- Mooncakes publication and full acceptance validation follow preliminary review.

## Reference

Independently implemented against https://mcap.dev/spec; reference ecosystem:
https://github.com/foxglove/mcap (MIT). No claim of a globally new file format.
