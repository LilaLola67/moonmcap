# 多话题同步与重采样

`synchronize` 以第一个输入的选定时钟为锚点，将其余输入逐行配对；
`resample` 在包含两端的固定时间网格上配对。支持 2–33 路同步输入，
或 1–32 路真实重采样输入。不会隐式重排所选时钟，观测到倒序即失败。

```sh
moon build --target js
node _build/js/debug/build/cmd/payload/payload.js sync input.mcap examples/synchronization/rules.json rows.ndjson
```

每路 `clock` 可以为 `log`、`publish`、`{"nanoseconds":"/stamp"}`，
或 `{"seconds":"/header/stamp/sec","nanos":"/header/stamp/nanosec"}`。
时间、容差、网格步长和行数上限使用十进制字符串，保留 UInt64 精度。
负秒数、超过 999999999 的纳秒分量、溢出及无效 JSON 均拒绝。
文件入口先扫描完整容器检查 CRC，再按话题解码和查询，输出原子发布到新文件。

`mode` 为 `nearest`（等距选较早时间）、`previous`、`next` 或 `linear`。
容差适用于真实样本距锚点的距离；线性插值要求两端均在容差内，不外推。
同一较早时间的重复记录取最后一条物理记录，未来时间取第一条。
`reuse:false` 是顺序贪心消费，避免复用已选样本，不是全局最优匹配；
它与线性插值互斥。`keep_missing:false` 丢弃不完整行且不消费该行的候选样本。

线性插值必须显式给出全局 `paths` 或每路 `paths`，只接受有限数值，
拒绝已知会舍入的整数转换。恰好命中时返回完整观测值，插值时只投影指定字段。
输出保留 `kind`、源时间、距离、括点记录的时间/序号/通道和插值分数，
因此派生值与真实观测可区分。分数的分子、分母是精确整数字符串，
数值计算使用浮点近似。

`buffer_bytes` 默认 8 MiB，限制算法保留的原始消息字节及固定消息开销；
不代表进程 RSS，也不包含临时解码对象、输出行或调用方自行保留的行。
`json_bytes` 默认 1 MiB，`rows` 默认 1000000，`input_messages` 默认 10000000。
达到行数上限会关闭输入，并在报告中标记 `limit_reached`；仅锚流耗尽标记
`anchor_exhausted`。库流调用方应在提前停止时 `cancel()`。

文件级验证：`python tools/synchronization_workflows.py`，使用独立 Python MCAP
写入器覆盖三种压缩、三话题、三种匹配方向、两种非日志时钟、插值括点与原子失败。
