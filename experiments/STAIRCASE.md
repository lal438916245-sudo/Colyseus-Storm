# Colyseus-Storm 阶梯并发实验

2026-11-12 已完成：[实验报告](../reports/staircase/实验报告.md)、[逐档汇总](../reports/staircase/consolidated-results.csv)、[轮次索引](../reports/staircase/结果索引.md)。已测最大通过档位为 2000 人/300 秒，RTT p95=139.27ms；2250 人/300 秒因 RTT 超标未通过。硬件、负载、保护记录和范围见报告，简历未修改。

使用真实 MMO 服务端与本机 MongoDB，经过真实 signup、关联 seat、业务认证、JOIN_ROOM 确认、完整初始状态与实际位置更新回执后，才计为在线玩家。服务端保留每房 150 人的设置，通过原有 joinOrCreate 自动分房。

本实验使用独立 Node/ws 长连接负载进程，持续读取入站帧、自动处理 ping/pong，每人每秒发送 10 次 `entityUpdate`。Locust 的 10–20 秒对局和登录冷却不适合直接表达固定在线人数，因此不把它的虚拟用户数当作本次实际在线并发。上述历史实验未修改原服务源码和简历；当前产品入口与 Locust 准入校验的更新见根目录 README。

## 复现

需要本机已有 MongoDB、原 MMO 服务端 Node 依赖，以及根目录 requirements.txt。推荐统一使用 `platform/.venv-locust`：`python storm.py verify --steps 4,10 --hold 12 --warmup 4 --workers 2 --spawn-rate 10 --profile-seconds 0`，自动完成以下审计和画图并返回 0/2/3 门禁。原始 step_load.py 入口同样默认自动完成审计、画图和报告。

```powershell
F:\Anaconda3\python.exe -m unittest discover -s experiments -p test_step_load.py -v

# 协议、判定、资源与清理预检
F:\Anaconda3\python.exe experiments\step_load.py --steps 4,10 --hold 12 --warmup 4 --workers 2 --spawn-rate 10 --profile-seconds 0

# 阶梯探索：每档 60 秒，4 个独立负载进程，20 人/秒递增
F:\Anaconda3\python.exe experiments\step_load.py --steps 20,50,100,150,200,300,500,750,1000,1500,2000 --hold 60 --warmup 10 --workers 4 --spawn-rate 20

# 确认阶段：将 N 替换为探索中通过的档位；新建服务与账号重新运行
F:\Anaconda3\python.exe experiments\step_load.py --steps N --hold 300 --warmup 15 --workers 4 --spawn-rate 20
```

## 预设判据

每档在所有目标连接完成业务准入后预热，再测量。通过需要：

- 实际在线数达到目标，观察期无掉线、协议错误或发送错误。
- 实际发送与服务端应用速率均不低于 `人数 × 10次/秒` 的 98%。
- 采样业务 ACK 覆盖率至少 99%，RTT p95 ≤ 200ms、p99 ≤ 500ms。
- 负载端调度丢拍比例 ≤ 1%、调度迟到 p95 ≤ 25ms，负载进程逐秒事件循环 p99 的时间分位 p95 ≤ 50ms。未达标时标记负载端有效性不足，不能直接推断服务器达到上限。

这些是本实验预先选定的 SLO，并非行业统一标准。CPU 利用率用于解释资源余量，不单独替代上述业务判据。CPU 的 100% 表示一个逻辑核心，不是整台 24 逻辑核机器全部满载。

## 测量方式

独立实验服务调用原始 `onAuth`、`onJoin` 和 `onEntityUpdate`，不替换业务。为测量真实业务往返延迟，在每 10 次位置更新后、原处理器已经应用坐标时，额外发出一次带序号、账号、session、房间与实际坐标的 ACK；首次位置更新也应答以验证准入。

客户端校验 ACK 内容并记录本进程单调时钟的 RTT，而不是把本地 `ws.send` 耗时叫作服务端响应时间。这是有固定采样观测开销的网络/服务端容量实验，不包含 Unity 渲染和完整客户端 Schema 解码。

每档的实际发送量、ACK 覆盖率按客户端测量阶段精确累计。服务端应用速率采用前后状态快照之差，采样边界会有毫秒级差异，因此可能略高于 100%；原始窗口时间和累计计数同时保存。CPU profile 在容量测量窗口结束后单独采集，避免把采样开销混入 SLO 结果。

## 原始产物

每轮输出到 `reports/staircase/<UTC时间_随机标识>/`：

- `manifest.json`、`source-snapshot/`：参数、版本、判据与实际运行代码哈希。
- `worker-*-events.jsonl`：每个账号的准入、初始状态、所有采样业务 ACK、异常、每秒负载端 CPU/内存/事件循环统计。
- `worker-*-accounts.jsonl`：本轮精确登记的测试账号，供清理审计。
- `server-events.jsonl`：真实认证、连接、终止、关闭，以及每秒在线数、房间人数、应用计数、出站字节、心跳、事件循环与 patch 耗时。
- `resource-samples.jsonl`：服务器、各压测进程、现有 MongoDB 进程的 CPU/RSS 与主机可用内存。
- `stage-N.json`、`workers-N.json`、`results.csv`、`summary.json`：逐档数据、采样 RTT 与汇总。
- `*.cpuprofile`：测量窗口之外的服务端 V8 CPU 调用栈采样。
- `cleanup.json`：专用服务和压测进程退出、本轮账号精确删除、输入源码未变。

运行结束默认自动核验并画图，以下命令供历史数据的独立复查使用：

```powershell
F:\Anaconda3\python.exe experiments\analyze_step_results.py reports\staircase\<轮次目录>
F:\Anaconda3\python.exe experiments\plot_step_results.py reports\staircase\<轮次目录>
```

`analyze_step_results.py` 将原始采样 ACK 与汇总逐条核对，重算 RTT 分位数，核对在线人数、身份、房间人数、源文件哈希和清理，并输出 `audit.json`、`secondary-metrics.json` 与 CPU profile 解读。预期失败的压力档也应能通过数据审计，数据有效与业务 SLO 通过是两回事。

调度迟到统计采用 0.1ms 上取整分桶，避免长档结束时对数百万条迟到值排序造成记录器内存膨胀。业务 RTT 仍保留所有采样原值、计算精确分位数；判据未放宽。`test_step_statistics.cjs` 验证分位误差方向与百万条数据的有限存储。

日志保留所有采样 ACK，不逐条打印全部 10Hz 发送以避免控制台成为瓶颈；完整发送/处理计数和序号保留在逐档及最终快照中。服务端原 demo 打印的测试密码已屏蔽。

## 运行隔离

每轮使用独立端口、专用服务进程和新的 `storm_step_<run_id>` 本机数据库，不加载原游戏环境配置。可用内存低于 900MiB，或本轮服务端与负载进程合计 RSS 超过 1,800MiB 时自动停止。结束时只关闭本轮创建的进程，按精确 username/email 删除本轮账号；不停止现有 MongoDB 服务。

容量结论必须注明硬件、工作负载、房间分布与观察时间。同机压测不是线上容量；达到最高设定阶梯也只表示验证到这个人数，不能自动称为服务器极限。短档探索之后应以独立较长运行确认。
