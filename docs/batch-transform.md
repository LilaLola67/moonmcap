# 批量日志变换

```sh
moon run cmd/mcap --target js -- transform examples/files/zstd-summary.mcap --output transformed.mcap --topic /imu --start 20000000000 --end 30000000000 --sample-period 4000000000 --sample-origin 20000000000 --rename /imu=/sensors/imu --align-start 100000000000 --compression zstd
python tools/batch_workflows.py
```

该示例从 20–30 秒闭区间选出 /imu，以 20 秒为采样原点，每 4 秒时间窗取第一条消息，得到原始 21、25、29 秒的三条记录，再将最早消息对齐至 100 秒，得到 100、104、108 秒。附件和元数据默认保留，附件时钟随对齐一起平移。

库接口为 `transform_log(log, options)`，配置从 `transform_options()` 更新，返回新 Log，原始输入不变。

| CLI 参数 | TransformOptions 字段 | 行为 |
| --- | --- | --- |
| --topic / --start / --end | query | 按原主题、闭区间时间筛选 |
| --rename FROM=TO，可重复 | topic_map | 同时映射名称；重复来源、缺失来源或空目标报错 |
| --sample-period NS | sample_period | 每通道每时间窗保留时间最早的一条；同时间按原始位置取第一条；0 关闭 |
| --sample-origin NS | sample_origin | 时间窗原点，采样启用时早于原点的消息不参与 |
| --descending / --offset / --limit | query | 筛选和采样完成后排序/分页 |
| --align-start NS | align_start | 将最终选中消息的最早 log_time 平移至目标；空消息集合不能对齐 |
| --attachment-name NAME，可重复 | attachment_names | 按名称列表保留附件，空列表不限制 |
| --attachment-media-type TYPE，可重复 | attachment_media_types | 按媒体类型列表保留附件 |
| --attachment-start / --attachment-end | attachment_start/end | 按原始附件 log_time 的闭区间筛选 |
| --metadata-name NAME，可重复 | metadata_names | 按名称保留元数据 |
| --metadata-match KEY=VALUE，可重复 | metadata_entries | 保留包含全部键值条件的元数据，保留其完整 entries |

处理顺序：验证输入/映射 → 按原主题和时间筛选 → 升序按通道采样 → 最终方向及分页 → 清理声明、映射主题及筛选附件/元数据 → 时间对齐 → 校验输出。采样使用 `[origin+n*period, origin+(n+1)*period)` 窗口，通过 UInt64 除法计算，不进行有溢出风险的窗末加法。

多个来源映射到同一主题时仍保留不同 channel ID、schema、编码和元数据；不静默混合不兼容声明。不同通道即使同主题，也独立采样。默认不去重消息、附件或元数据。

时间对齐对所有选中消息的 log_time/publish_time 和附件的 log_time/create_time 施加同一平移。任何字段溢出/下溢则整个操作报错，不截断、不钳制。附件筛选发生在对齐之前。

本页的 `transform_log` 与 `transform` 路径使用内存 Log，受完整输入文件预算限制。较大输入可使用已经提供的 [流式变换](stream-transform.md) 与 [磁盘外部归并](external-merge.md)，按相应接口的预算运行。写出重建声明引用、Statistics、索引及 CRC，并由官方读者检查。
