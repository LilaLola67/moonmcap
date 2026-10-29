# 载荷编码与字段修改示例

下列定义和数据为合成数据，输出路径须尚不存在。仓库根目录执行：

```sh
moon run cmd/payload --target js -- encode cdr examples/payload-rewrite/value.msg demo/Value examples/payload-rewrite/value.json value.bin
moon run cmd/decode --target js -- raw-cdr examples/payload-rewrite/value.msg value.bin demo/Value --int64-strings
moon run cmd/payload --target js -- raw-rewrite cdr examples/payload-rewrite/value.msg demo/Value value.bin examples/payload-rewrite/rules.json changed.bin
moon run cmd/decode --target js -- raw-cdr examples/payload-rewrite/value.msg changed.bin demo/Value --int64-strings
```

第一次读取的 frame_id 为 `sensor`，第二次为 `base_link`；counter 均为精确的 `9007199254740993`，data 均为 `[0,255,7]`。完整 MCAP 回写、字段删除和未知 Protobuf 字段保留范围见[使用说明](../../docs/payload-rewrite.md)。
