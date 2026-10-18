# 消息游标

MoonMCAP 0.3.0 在已有内存时间索引上增加连续遍历接口。一次创建游标后可以逐条或分批取消息，无需每翻一页重新收集、排序全部候选结果。它适合日志浏览、回放与分批处理。

## 运行示例

```sh
moon run cmd/cursor
moon run cmd/cursor --target js
```

示例实际写入、读取一份 Zstd 分块文件，再演示分页、包含边界的时间跳转、取消和倒序查询。预期输出：

```text
page 1: sequence=[5, 1]
page 2: sequence=[2, 3]
seek 20 inclusive: sequence=[2, 3, 0]
after seek: matched=4, emitted=3, remaining=1
reverse: sequence=[4, 0, 3]
cursor example: PASS
```

## 使用

在已导入 `"LilaLola67/moonmcap" @mcap` 的包内：

```moonbit
let index = @mcap.build_query_index(log)
let cursor = index.cursor({
  ..@mcap.all_query(),
  topics: ["/imu"],
  start_time: 100UL,
  end_time: 500UL,
  offset: 10,
  limit: 200,
})
let first_page = cursor.next_page(32)
let next_message = cursor.next() // Message?; None 表示结束或取消
let progress = cursor.progress()
cursor.seek(300UL) // 从 300 开始，包括时间恰好为 300 的消息
cursor.cancel()
```

`cursor`、`next_page` 和 `seek` 会抛出 `McapError`；调用处需要声明错误或处理异常。`next`、`progress` 和 `cancel` 不抛出该错误。

## 顺序与状态约定

- 主题筛选按 topic 匹配，可以选中同主题的多个 channel；重复筛选项不重复消息。
- 时间区间两端包含，时间保留 UInt64 精度。升序的同时间消息按输入 Log 的物理位置排序；倒序是整个升序序列的反转，包含同时间消息的顺序。
- `offset` 和 `limit` 作用于整个遍历，分页时不重新计算。`next_page(size)` 最多返回 size 条，size 必须大于零；最后一页可以不足 size。返回空页或 `next()` 返回 None 表示当前遍历结束。
- `seek(time)` 的 time 必须在原始查询区间内。升序从 time 向原始 end_time 遍历，倒序从 time 向原始 start_time 遍历；二者均包括 time。跳转重置进度与 limit，不重新应用 offset。
- 已结束的游标可以 seek 重新开始；跳转可能再次返回此前读过的消息。游标是进程内对象，不是可序列化的跨进程分页令牌。
- `cancel()` 可重复调用，取消后不再返回消息且不能 seek 重开。取消在调用间生效，不提供线程抢占或异步 I/O 取消。
- 非法查询、页大小或跳转参数抛出错误，不改变原游标位置。

`progress()` 提供以下诊断：

| 字段 | 含义 |
| --- | --- |
| state | Active、Exhausted 或 Cancelled |
| matching_messages | 当前时间窗口内匹配的总数，不扣除 offset 或 limit |
| skipped_messages | 本次遍历应用的 offset，最多为匹配总数；seek 后为零 |
| emitted_messages | 本次遍历已经返回的消息数 |
| remaining_messages | 考虑 offset 和 limit 后仍可返回的数量；取消后为零 |
| buffered_heads | 等待归并的有序段数；不是字节数或进程内存测量 |

游标保存创建时主题条件的副本，并共享 QueryIndex 已拥有的快照。后续修改原始 Log、筛选数组或某一页的数组不会改变游标结果；同一个索引可创建多个互不推进的游标。消息字段和 Bytes 是不可变值，可以安全共享。

## 成本与边界

无主题筛选时，对全局时间索引二分定位并保存一个有序段；有主题筛选时，对各命中 channel 二分定位，并用二叉堆归并，最多保留每个非空 channel 的一个头部。设命中通道数为 k，除筛选条件副本外，游标额外状态为 O(k)，每条消息归并成本为 O(log k)，一页结果另占 O(page_size) 空间；全局单段遍历每条为 O(1)。大 offset 仍需逐条跳过，不宣称常数时间随机页访问。主题匹配还需遍历声明表。

QueryIndex 构建仍会验证并保留完整 Log、建立全局及通道位置数组。该内存 API 文件需先 `read_log`，取消后仍持有索引快照。文件按需分页使用另一个 [磁盘游标 API](file-cursor.md)，乱序多文件有界负载排序使用 [外部合并](external-merge.md)。

测试将不同页大小、方向、主题、offset、limit 的结果与原批量查询对照，并覆盖跨通道同时间消息、空结果、时间边界、非法操作原子性、独立游标、取消、随机数据及三种压缩文件往返。
