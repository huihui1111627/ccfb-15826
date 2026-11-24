# 哈希连接运行时过滤器（Runtime Filter）演示组件

针对蚕种场“几亿行检验记录表 × 几千行品系表”的哈希连接场景：小表哈希表构建完成后，把构建端
实际出现的连接键做成运行时过滤器，下推到大表扫描端，在扫描阶段就丢弃不可能连接的行，避免
把大表整表扫完再连接。

## 实现的 8 项需求

| # | 需求 | 实现位置 |
|---|------|----------|
| 1 | 构建端实际键做成过滤器；键少用精确集合、多用布隆，并附 min/max | `rf/runtime.go`（ShardBuilder/MergeRuntimeFilters）、`rf/filters.go` |
| 2 | 过滤器沿计划树下传，可穿过投影/过滤/另一个连接；外连接保留侧阻断 | `exec/pushdown.go`（joinChildrenPushable、remapThroughProject） |
| 3 | 超尺寸退化为只发 min/max 区间 | `rf/runtime.go` finalize/degrade |
| 4 | 探测端可等待过滤器，等太久先不过滤，到达后再启用 | `rf/handle.go`（Handle.Wait 超时）、`exec/scan.go`、`exec/hashjoin.go`（构建/扫描流水线） |
| 5 | 多个并行构建分片各自生成过滤器再合并 | `exec/hashjoin.go` runBuild（按 key 哈希路由分片）、`rf/runtime.go` MergeRuntimeFilters |
| 6 | 运行中统计实际滤除比例，过低则自适应停用 | `exec/scan.go` rfAdaptive |
| 7 | 连续整数键改用位图，探测端按位判断 | `rf/filters.go` bitmapFilter、`rf/runtime.go` MaybePromoteBitmap |
| 8 | 组合键、字符串键、NULL 连接语义 | `rel/value.go` EncodeKey 自描述编码、`rf/runtime.go` NullPasses、`exec/hashjoin.go` 外连接 NULL 处理 |

## 四种过滤器物理形态

- `EXACT`：精确键集合（map），零误判，构建端 distinct ≤ 阈值时使用
- `BITMAP`：单整数列且取值连续稠密时由精确集合自动升级，O(1) 按位判断
- `BLOOM`：distinct 超阈值时使用，双哈希定位，无假阴性，理论/实测误判率都统计
- `MINMAX`：尺寸超预算退化形态，只保留每列 [min,max]，用于块级裁剪

## 目录结构

```
rel/    值类型、复合键编码、表/块/块级 min-max 统计
rf/     过滤器（精确/位图/布隆/区间）、分片合并、异步句柄、自适应停用
plan/   扫描/过滤/投影/哈希连接/嵌套循环计划树
exec/   执行器、过滤器下推算法、并行分片构建、外连接语义
gen/    固定随机种子的 27 种表与查询（倾斜/外连接/多层连接/大构建端/聚簇…）
verify/ 三模式对拍（无过滤 / 运行时过滤 / 嵌套循环基准）与指标采集
web/    可视化页面
main.go CLI 与 HTTP 服务入口
```

## 运行

```bash
# 跑全部 27 个场景的三方对拍并打印指标，不启动网页
go run . -no-serve

# 启动可视化面板（默认 http://127.0.0.1:8080）
go run .

# 导出 JSON 报告
go run . -no-serve -json report.json

# 只跑某个场景 / 换种子 / 收紧过滤器尺寸上限
go run . -no-serve -only 26
go run . -no-serve -seed 99
go run . -no-serve -max-filter-bytes 4096
```

页面内容：查询计划树、每个过滤器的下推链路（从哪生成→穿过哪些算子→推到哪个扫描/为何被
阻断）、过滤器大小、理论与实测误判率、行级滤除率、块级跳过数、晚到行数、各算子构建/探测
耗时，以及三种执行模式的耗时与结果行数对比。

## 正确性保证

每个场景用固定种子生成，分别以三种方式执行：

1. 哈希连接，不开运行时过滤
2. 哈希连接，开运行时过滤
3. 嵌套循环连接（独立参考实现）

三者结果按多重集（排序后逐行）比较，必须完全一致。`go test ./...` 覆盖：三方对拍、确定性、
位图升级、布隆无假阴性、分片合并、退化、自适应停用、异步晚到、块级裁剪、外连接阻断规则。

## 关键设计说明

- **分片路由必须与探测一致**：构建行按 `hash(key) % nshards` 进入固定分片通道，探测按同一
  公式查同一分片，否则会漏匹配。
- **外连接规则**：内连接两侧都可过滤；左外连接只可过滤右侧、右外只可过滤左侧、全外两侧都
  阻断；过滤器要穿过路径上的另一个连接时，同样检查该连接的保留侧。
- **NULL 语义**：等值连接中 NULL 永不匹配；过滤器默认拒绝 NULL 键，仅当探测侧是该外连接的
  保留侧时才放行（NullPasses），保留侧的 NULL 行仍按外连接语义输出。
- **流水线**：构建在后台 goroutine 进行，探测侧泵协程持续扫描；扫描在过滤器句柄上做有界
  等待，超时后未过滤的行计入 LateRows，过滤器到达后立即生效。
