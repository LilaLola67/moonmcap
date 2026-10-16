// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "LilaLola67/moonmcap"

version = "0.1.0"

readme = "README.md"

repository = "https://github.com/LilaLola67/moonmcap"

license = "MIT"

keywords = [ "mcap", "robotics", "binary", "logging", "index" ]

preferred_target = "wasm"

description = "MoonBit-native MCAP message-log codec, indexed queries and file transformations"

import {
  "bikallem/compress@0.3.4",
  "bikallem/blit@0.2.2",
}
