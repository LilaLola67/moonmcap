# 文档导航

从 [首次使用](quickstart.md) 开始；选择入口前阅读 [支持范围](scope.md)。
发生错误时查 [排错](troubleshooting.md)，需要复核交付质量时查 [测试方法](testing.md)。
生成的公开接口索引见 [Mooncakes API](https://mooncakes.io/docs/LilaLola67/moonmcap)。

| 目标 | 文档 |
| --- | --- |
| 设计、合同与验收 | [架构和所有权](architecture.md)、[API 合同与错误](api-contracts.md)、[统一验收入口](verification.md) |
| MCAP 格式、压缩和资源边界 | [兼容范围](compatibility.md)、[实测与 RSS](performance.md)、[来源和许可](../third_party/README.md) |
| 安装、API 和版本升级 | [项目入口](../README.md)、[发行包](distribution.md)、[迁移](migration.md)、[更新记录](../CHANGELOG.md) |
| 查询、裁剪、合并、完整诊断 | [CLI](cli.md)、[文件样例](../examples/README.md)、[诊断修复](diagnostics.md)、[大文件诊断](source-diagnostics.md) |
| 选择性 IO 和磁盘索引 | [RangeReader](range-reader.md)、[侧车](sidecar-index.md)、[分页索引](disk-sidecar.md)、[字段索引](field-index.md)、[分页字段管线](pipeline.md) |
| 大文件处理与中断恢复 | [外部合并](external-merge.md)、[恢复任务](resumable-merge.md)、[Chunk 保存点](chunk-checkpoint.md)、[贯通恢复](chunk-pipeline.md)、[流式片段](stream-context.md) |
| 异步宿主、网络和并行 | [异步 IO](async-io.md)、[统一查询](pipeline.md)、[HTTP](http-source.md)、[缓存](http-cache.md)、[工作线程](parallel-codecs.md) |
| 消息内容和字段演进 | [解码](payload-decoding.md)、[CDR](cdr-decoding.md)、[ROS1](ros1.md)、[回写](payload-rewrite.md)、[结构兼容与归一化](schema-evolution.md) |
| 时间与质量处理 | [质量和事件](incident-workflows.md)、[同步](synchronization.md)、[TF](tf.md)、[增量监控](live-tail.md)、[容差回归](regression.md) |
| 可选工具集成 | [列式导出](columnar.md)、[设备 SDK 桥接](device-bridge.md)、[处理配方](recipes.md) |
| 数据与可复现证据 | [公开语料说明](corpus-validation.md)、[文件清单](../examples/manifest.json)、[历史验证报告](../examples/validation/release-0.16.9.json) |

专门接口的选项和限制以各文档及生成 API 为准。示例使用的合成数据、公开录制、
转换数据和真实设备联调分别标明，不从其中一种验证推断其他场景已经覆盖。
