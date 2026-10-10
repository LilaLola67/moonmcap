# 公开项目查重记录

检索日期：2026-10-10。结论：在以下公开检索范围内，未发现本项目以外的 MoonBit MCAP 核心读写、索引与日志变换库。该结论不等于证明全网绝无重复；GitHub 索引、描述缺失、私有仓库及未来新增项目均有局限。

## GitHub

通过 GitHub 官方仓库搜索 API 检索，结果数量包含本项目：

| 查询 | 数量 | 人工核对 |
| --- | ---: | --- |
| `mcap language:MoonBit` | 1 | 仅 `LilaLola67/moonmcap` |
| `mcap MoonBit in:name,description,readme` | 2 | 本项目与 `hoilc/scoop-lemon`；后者是软件安装清单，非同类库 |
| `moonmcap` | 1 | 仅本项目 |
| `rosbag MoonBit in:name,description` | 0 | 无结果 |

复查入口：[GitHub MCAP + MoonBit 搜索](https://github.com/search?q=mcap+language%3AMoonBit&type=repositories)。

另行核对相邻项目 [rsasaki0109/BagDrop](https://github.com/rsasaki0109/BagDrop)：浏览器本地 ROS2 SQLite `.db3` 日志检查应用，含 MoonBit 分析组件。本项目定位为可复用 MCAP 二进制基础库，交付读写、索引、CRC、记录流解码及日志变换 API，不实现 SQLite/CDR 浏览器分析界面。二者涉及机器人日志场景，但文件格式、核心实现和交付物不同。

## mooncakes.io

读取 [官方模块列表 API](https://mooncakes.io/api/v0/modules)：2,909 条版本记录、2,907 个唯一模块名。对名称、描述及关键词以不区分大小写的 `mcap|rosbag` 检索，匹配为 0。仅基于登记元数据，不能排除未说明功能的包。未将无关的 CRC、UTF-8、容器或 SQLite 基础组件视为同题项目。

## 原创和参考边界

- 格式来自 [MCAP 公共规范](https://mcap.dev/spec)，不是本项目发明。
- [foxglove/mcap](https://github.com/foxglove/mcap) 已有多语言实现，许可证 MIT。本项目是独立编写的 MoonBit 实现，非上游源码逐行移植。
- `tools/interop.py` 使用官方 Python `mcap==1.3.0` 作测试参考；测试夹具由其生成，单独标注，排除在有效实现源码量外。
- 本项目采用 MIT；运行核心仅依赖 MoonBit 标准库。第三方参考实现和本地测试依赖不作为本项目原创源码提交。

报名定位：补充 MoonBit 生态中的 MCAP 日志基础能力，避免宣称全球首创、已有格式原创或检索绝对无遗漏。
