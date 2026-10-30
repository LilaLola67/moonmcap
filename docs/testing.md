# 验证方法与证据

测试按风险组织，数量不代表支持范围。固定基准是 moonc 0.10.17+d971670cd、
Node.js 22.3+；CI 的参考工具使用 Python 3.12，依赖版本写在工作流中。

| 需要证明什么 | 可运行检查 | 能证明的范围 |
| --- | --- | --- |
| 编码与算法边界 | `moon test --target wasm`、`moon test --target js` | 格式拒绝、UInt64、排序、状态机和预算；JS 另覆盖宿主 |
| 独立容器互操作 | `python tools/conformance.py`、`python examples/run.py` | Python MCAP writer/reader、三种压缩与可用 CRC；不要求压缩字节完全相同 |
| 用户可见失败合同 | `python tools/reliability_workflows.py` | 参数在 IO 前失败、精确预算、晚发错误原子性、stdout 部分结果、完整/选择性校验区别 |
| 文档可以使用 | `python tools/check_docs.py`、`python tools/quickstart_smoke.py` | 本地链接与锚点、隔离源码树中的真实命令、README API 示例 |
| 任务中断恢复 | `python tools/recording_session_workflows.py`、`python tools/clip_batch_workflows.py` | 实际终止进程、已提交内容复用、重复恢复；不声称通过断电测试 |
| 已发布包可安装 | `python tools/install_smoke.py --version 0.17.1` | 新使用者目录从 Mooncakes 安装，无源码路径覆盖，并编译使用方自己的 worker |
| 公开采集数据 | `python tools/corpus_workflows.py --public` | 固定大小和 SHA-256 的公开文件；网络或指纹失败不能算通过 |

常规 CI 执行检查、构建、单元测试、离线互操作与故障工作流，并保存 JSON 报告。
`large_source` 和 `public_corpus` 是手动选择的工作流：未勾选显示 skipped，不能
把它们写成每次提交都已执行。公开数据和大规模验证的历史报告保留日期、来源、
环境和规模，当前版本的结果与历史观测分别记录。

新源码导出检查使用 Git 索引的文件树，不包含 `.git`、`.mooncakes`、`_build`、
`_interop_deps` 或本地生成报告；仍会使用已安装的编译器及其下载缓存，不宣称全新操作系统。
导出树和所有临时结果位于 `_interop/quickstart/`，不覆盖工作区文件。

独立参考工具的安装：

```sh
python -m pip install mcap==1.3.0 lz4==4.4.5 zstandard==0.25.0 protobuf==6.33.0 rosbags==0.11.0
```

Arrow、设备桥接的专门验证另需 PyArrow 21.0.0、foxglove-sdk 0.30.3、
websockets 15.0.1；它们不属于核心库运行依赖。普通调用不需要安装全部参考工具。

发布检查依次核对：源树/许可证与包清单、目标提交 CI、实际发布账号、服务端构建、
全新使用者安装和在线文档。上传返回成功不等于后续步骤已经完成。
执行失败的检查保留日志，不以删除断言或跳过失败场景使结果通过。
