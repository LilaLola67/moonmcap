# 可复现处理配方

处理配方把 MCAP 校验、归一化、列式导出、质量检查和实验比较串成显式步骤，
适合重复跑多次实验、交换处理流程，以及中断后复用已完成的输出。
它是可选 Python 调度工具，容器处理仍调用 MoonBit 命令；不执行任意 shell。

```sh
moon update
python tools/recipes.py examples/recipes/clip.json _interop/my-recipe --stop-after 1
python tools/recipes.py examples/recipes/clip.json _interop/my-recipe --resume
```

示例使用仓库自带合成 MCAP，先生成类型清单，再流式裁剪，并对片段重新校验。
job 路径必须全新；父目录须存在。安装参考依赖后，
`python tools/recipe_workflows.py` 演示六步流程，包括实际 Parquet 导出与读回。
列式步骤需要 `pyarrow==21.0.0`，其余步骤不要求该依赖。

## 配方与来源

version 固定为 1。inputs 声明本地文件的 path 与 kind（mcap/file），rules
把规则名称映射到 JSON 文件；这些路径相对配方文件。steps 的 id 唯一，
依赖只能指向输入或前面的步骤，从而排除环。input 接受单输入步骤；
regression 使用 baseline/candidate。rules 指定对应规则名称，inventory 无需规则。

支持 normalize、rewrite、sync、tf-query、tf-clip、inventory、clip、quality、
events、columnar、regression。每步直接复用对应工具文档中的配置格式；
events 生成事件报告，clip 生成指定窗口，不额外隐式生成事件片段。
regression 的事件文件与证据 MCAP 必须通过 bindings 声明资源，例如
`"bindings":{"/evidence/baseline_mcap":"source","/evidence/candidate_mcap":"source"}`，
不允许规则偷偷引用未绑定文件。上一步单一主输出可以作为后一步输入。

plan.json 保存配方原字节散列、每个输入的大小和 SHA-256、每份规则的原始及
规范化散列、MoonBit/Python/Node 与可选适配器版本、项目代码及编译入口散列。
MCAP 输入先执行完整容器校验，并记录 schema 数据散列和 channel 类型清单散列。
plan 中不写绝对输入路径或机器名称。每个输出收据记录文件清单、大小、散列和
整棵输出目录的散列；它们构成来源记录，并非带签名的防恶意篡改证明。

## 继续与失败

state.json 原子替换并带完整性散列。步骤先在 staging 中完成，保存 durable
pending 收据，再改名到 artifacts，最后登记完成。两侧的中断均可在恢复时
校验并接续。完整输出从不重写；未完成 staging 移到同一 job 的 abandoned
目录后重做。恢复拒绝输入、规则、配方、代码、运行环境或已完成输出变化，
需要新建 job 才能跑改变后的实验。初始化只有在 plan 匹配且没有输出时才恢复。

PID 锁拒绝活动任务，死进程锁可恢复；若遗留子进程仍存活则等待其退出后再恢复。
锁恢复保护目录若自身遭遇中断，需先检查现场。工具不保证任意操作系统强杀时
所有子进程立即退出，不提供外部副作用的恰好一次语义。此配方只发布本地文件。
每条命令最长一小时，日志保存在 job/logs；错误时保留现场供排查。

quality/regression 的业务失败仍保存完整报告，最终 analysis_status 为 failed，
完整任务退出 1；执行或配置失败退出 2；成功或主动分段停止退出 0。
其他正常完成步骤不会因业务失败而被偷偷省略。

最多 128 输入、128 规则、64 步；配方/规则各 1 MiB，检查点 8 MiB。
每步最多发布 256 文件，默认发布字节预算 10 GiB，可用 artifact_bytes 调整。
该预算在步骤完成时核对，不能限制运行中临时磁盘或总 RSS；底层步骤的预算
仍分别生效。abandoned 和日志不会自动清理，原始输入、编译器和依赖仍需保留。
