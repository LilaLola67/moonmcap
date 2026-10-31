# MoonMCAP

MoonBit 的 **MCAP 0 消息日志库**，用于读取和校验机器人、传感器与仿真日志，按主题和时间查询，并裁剪、合并或恢复可读数据。当前版本 **0.17.2**。

从 [首次使用](docs/quickstart.md) 开始，选择入口前阅读 [支持范围](docs/scope.md)。完整索引见 [文档导航](docs/README.md)，异常处理见 [排错说明](docs/troubleshooting.md)。

## 安装

项目固定编译基准为 **moonc 0.10.17+d971670cd**，满足不低于 0.10.16 的要求；另以当前官方工具链验证兼容性。本地文件和网络宿主需要 **JS/Node.js 22.3+**。

在已有 MoonBit 项目中执行下面的安装命令；从空目录开始时，先创建 `moon.mod`：

```text
name = "example/use-mcap"
version = "0.0.1"
```

```sh
moon add LilaLola67/moonmcap@0.17.2
```

使用方的 `moon.pkg`：

```text
import { "LilaLola67/moonmcap" @mcap, }
pkgtype(kind:"executable")
```

将下面保存为 `main.mbt`，执行 `moon run .` 或 `moon run . --target js`：

```moonbit
///|
fn main {
  try {
    let recorder = @mcap.new_builder()
    let channel = recorder.register_channel("/events", "json")
    recorder.append(channel, 100UL, b"{\"value\":1}")
    let bytes = recorder.finish(compression=@mcap.Zstd)
    let messages = @mcap.query_file(bytes, {
      ..@mcap.all_query(), topics: ["/events"], start_time: 100UL, end_time: 100UL,
    }).messages
    assert_eq(messages.length(), 1)
    assert_eq(messages[0].data, b"{\"value\":1}")
    println("MCAP_SAMPLE_OK")
  } catch {
    @mcap.Invalid(reason) => abort(reason)
    error => abort("\{error}")
  }
}
```

预期输出 `MCAP_SAMPLE_OK`。库中的时间是 **UInt64 纳秒**，查询区间包含两端。
JSON 时间使用字符串保留精度；不根据设备字段名推断单位或时钟关系。

## 文件处理

```sh
git clone https://github.com/LilaLola67/moonmcap.git
cd moonmcap
moon update
moon build --target js
node _build/js/debug/build/cmd/mcap/mcap.js query examples/files/zstd-summary.mcap --topic /imu --start 20000000000 --end 30000000000 --full
```

应查询到 **5 条消息**，时间为 21、23、25、27、29 秒。完整的裁剪、校验和截断恢复命令及预期结果见 [首次使用](docs/quickstart.md)。示例文件随仓库提供，不需要实体设备。

## 能力与边界

- **核心库（Wasm/JS）**：MCAP 0 标准记录、CRC、索引、无压缩/LZ4/Zstd、查询、变换与恢复。压缩字典、拼接帧和未声明的格式扩展不在支持范围，详见 [格式兼容表](docs/compatibility.md)。
- **宿主与大文件路径（JS/Node）**：按需读取、磁盘索引、流式输出、外部排序、持久任务、HTTP 范围读取和可选工作线程。它们分别有预算，不能把单个预算视作总进程内存上限。
- **消息与外部适配**：JSON/Protobuf/ROS1/plain XCDR1 解码及回写、时间与质量处理按各自文档约束使用。Python/PyArrow/Foxglove 工具为可选适配器，不属于核心运行依赖。

快速查询不等于完整校验；需要审核全文件时使用 `validate` 或 `--full`。普通查询/裁剪默认最多 **100000 条**，内存型文件处理默认编码输入预算 **64 MiB**。已有输出不覆盖；文件发布需要同目录硬链接支持。流式 stdout 失败时可能已有部分结果，需检查退出码。完整约定见 [使用边界](docs/scope.md)。

## 按目标查文档

| 目标 | 入口 |
| --- | --- |
| 查询、裁剪、合并和修复 | [CLI](docs/cli.md)、[可运行样例](examples/README.md)、[诊断](docs/source-diagnostics.md) |
| 选择性 IO、大文件和恢复任务 | [RangeReader](docs/range-reader.md)、[索引](docs/disk-sidecar.md)、[流式片段](docs/stream-context.md)、[外部合并](docs/external-merge.md) |
| 解释或修改消息 | [解码](docs/payload-decoding.md)、[回写](docs/payload-rewrite.md)、[结构演进](docs/schema-evolution.md) |
| 时间、质量和实验处理 | [同步](docs/synchronization.md)、[TF](docs/tf.md)、[质量](docs/incident-workflows.md)、[回归](docs/regression.md) |
| 可选工具集成 | [列式导出](docs/columnar.md)、[设备桥接](docs/device-bridge.md)、[处理配方](docs/recipes.md) |

## 验证与数据来源

```sh
moon check
moon build
moon test --target wasm
moon test --target js
python tools/check_docs.py
python tools/quickstart_smoke.py
```

[测试方法](docs/testing.md) 区分单元测试、独立参考实现、真实进程中断、干净源码副本、发行包安装和公开数据验证；[GitHub CI](https://github.com/LilaLola67/moonmcap/actions/workflows/ci.yml) 保存可复核的报告。公开录制、转换数据和合成样例分别标注，见 [公开语料](docs/corpus-validation.md)；历史百万消息和大文件实测的环境与限制见 [性能报告](docs/performance.md)。设备 SDK 桥接尚未进行实体设备联调。

本次改进的范围和独立验证记录见 [0.17.2 质量记录](examples/validation/release-0.17.2.json)，完整验证命令及证据判读见 [验收入口](docs/verification.md)。历史记录见 [0.17.1 质量验证](examples/validation/release-0.17.1.json)。

历史验证记录保留在 [验证报告](examples/validation/release-0.16.10.json) 等文件中，版本变化见 [更新记录](CHANGELOG.md) 与 [迁移说明](docs/migration.md)。

从 Mooncakes 全新安装的验证命令为 `python tools/install_smoke.py --version 0.17.2`，不使用仓库源路径覆盖。

## 开源许可

项目主体采用 [MIT](LICENSE)。压缩与字节缓冲的适配组件保留上游 Apache-2.0 许可和来源，见 [第三方说明](third_party/README.md)；这些引用实现不计入原创核心源码统计。公开录制文件遵循其发布方的条款，项目 MIT 许可不对它们重新授权。
