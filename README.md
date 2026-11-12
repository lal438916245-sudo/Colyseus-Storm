# Colyseus-Storm

面向 Colyseus MMO Demo 的协议级游戏压测工具。以 `storm.py` 为统一入口，完成真实认证、房间准入、业务发包、自动判定、原始数据审计、绘图和账号回收。无需启动 Unity 客户端。

## 已实现的流程

```text
参数与环境检查 → 独立服务/数据库 → 注册与 seat → 认证与进房
→ 业务负载与监控 → 停止/回收 → 原始日志审计 → 图表与 HTML → 退出码门禁
```

- **固定阶梯验证（verify）**：按指定人数递增，完成预热和测量；首次不通过即停止，区分服务器业务 SLO 与负载端有效性。
- **容量探索（search）**：倍增探索、区间细化，再用独立长档确认；短档结果长档未通过时回退较低档位。每次探测使用新的服务和账号。
- **严格玩家生命周期（locust）**：注册 → 消费 seat → WebSocket → JOIN_ROOM 原始确认字节 `0x0A` → Schema 首个完整状态 → 校验自身 sessionId/账号 → 发包 → 退房 → 登录获取新 seat。ERROR=11、初始状态超时、身份不匹配均失败。
- **自动审计**：容量实验以原始 ACK 日志重算分位数，核对身份、在线人数、发送/应用计数、源码快照与清理；Locust 核对成功进房、发送/服务端应用计数和停止钩子清理。
- **自动产物**：每轮独立目录，包含 `report.html`、`summary.json`、`audit.json`、图表、原始日志、账号登记与 SHA-256 清单。
- **回收与取消**：正常结束自动回收；取消触发清理；控制器被强制杀死时可按运行目录补偿回收。仅处理 PID+创建时间匹配的自有进程和精确登记的账号。

## 环境准备

已在 Windows / Python 3.13 / Node.js 24、Colyseus 0.14.13 MMO Demo 上本地验证。需要本机 MongoDB（127.0.0.1:27017），以及游戏服务端目录中已安装的 Node 依赖。脚本不启动或停止 MongoDB 服务。

在本仓库目录运行；统一游戏测试平台的现有虚拟环境可直接复用：

```powershell
$py = 'F:\测试平台\platform\.venv-locust\Scripts\python.exe'
& $py -m pip install -r requirements.txt
$env:STORM_SERVER_ROOT = 'F:\Colyseus_SDET_Project\colyseus-unity-demo\colyseus-game-client\Server'
$env:STORM_NODE = 'F:\Colyseus_SDET_Project\Node\node.exe'
```

其他机器请替换路径。未使用平台时，可用 `python -m venv .venv` 创建环境并安装同一份 requirements；Linux 可设置 `STORM_NODE=node`。服务端需保留本 Demo 的认证、房间、数据库结构和对应 Schema 包；这不是任意 Colyseus 游戏的零配置适配器。

## 一键执行

### 固定阶梯验证

```powershell
& $py storm.py verify --steps 20,50,100 --hold 60 --warmup 10 --workers 4
$LASTEXITCODE
```

`--steps` 可配置至 5,000 人，不固定为 2,000 人。先使用少量玩家确认环境，再按机器资源增大；默认保护条件为可用内存低于 900MiB，或本轮服务与负载进程合计 RSS 超过 1,800MiB。保护触发返回 3，不能据此宣布服务端人数上限。

### 自动容量探索

```powershell
& $py storm.py search --start 100 --max-users 2500 --precision 100 --hold 60 --confirm-hold 300 --min-capacity 1000
```

`--max-users` 是允许探索的上限，`--precision` 是人数区间的停止精度。输出 `confirmed_capacity`、`failed_upper` 和 `search_status`。达到设置上限仍通过时记录 `lower_bound_only`，表示“至少通过到该人数”。搜索中预期的过载档不导致整个探索失败；最终根据数据有效性、清理和 `--min-capacity` 判定。发压端失效立即返回 3，不当作服务器失败上界。

### 严格玩家生命周期

```powershell
& $py storm.py locust --users 10 --spawn-rate 2 --run-time 90s
```

持续时间足够时，玩家退房后通过 login 获取新的 seat 再进房，遵守原服务端登录冷却。保留 Locust HTML/CSV，额外生成统一审计、图表和报告。

Locust 的本地 `send` 耗时仅表示发送调用耗时；容量模式的业务 RTT 来自服务端实际应用位置更新后的采样 ACK，两者不能混用。容量实验每玩家每秒 10 次更新、每 10 次采样一次 ACK，房间容量保留原 Demo 的 150 人。

## 退出码契约

| 返回码 | 含义 |
| --- | --- |
| 0 | 验收目标通过，数据审计和清理通过 |
| 2 | 数据有效，但业务 SLO 或最低容量目标未达标 |
| 3 | 执行失败、发压端失效、原始数据审计失败、绘图失败或清理不完整 |

固定阶梯默认要求：在线人数达到目标、无掉线/协议错误，实际负载和应用速率 ≥98%，采样 ACK 覆盖 ≥99%，业务 RTT p95 ≤200ms / p99 ≤500ms；同时检查调度迟到、丢拍与负载进程事件循环。阈值是本项目的实验约定。可用 `--p95-limit-ms` / `--p99-limit-ms` 明确调整并记录。

CI 应直接使用命令返回码。即使已经生成图表或审计通过，业务 SLO 失败仍返回 2；无有效测量、只完成部分执行或清理失败均不会通过。

## 查看报告与恢复

控制台输出 `STORM_RESULT`，指向本轮目录；可通过 `--output reports/your-new-run` 指定一个**尚不存在**的目录，避免覆盖历史结果。

- `report.html`：统一入口，图表内嵌，单独下载也能显示。
- `summary.json`：模式、逐档结果、门禁、审计和清理结论。
- `audit.json`：原始数据核对结论；`ARTIFACTS.sha256.json`：最终文件校验清单。
- `manifest.json` / `source-snapshot/`：输入参数、版本与源码快照；search 中每次探测单独保留。
- `ownership.json`、账号 JSONL、`cleanup.json`：资源归属和清理证据；强制中断后的补偿记录为 `recovery.json`。

正常 Ctrl+C 触发取消。也可在运行目录创建 `cancel.requested`。若控制器被强制结束，运行：

```powershell
& $py storm.py recover --output reports/your-run
```

恢复命令返回 0 仅表示自有资源回收成功，不会把中断的测试改成通过；没有最终报告时补生成 Exit 3 的报告。再次执行回收可以复核无残留。不删除整个游戏数据库或他人账号。

需要重新分析历史数据时，保留原目录副本后再使用 `experiments/analyze_step_results.py` 和 `plot_step_results.py`；单独重分析不替代主流程的最终门禁。

## 统一游戏测试平台

计划中选择 `locust` 引擎、**Colyseus-Storm 一键闭环**场景（引用 `colyseus-storm`），使用 `offline` Environment 与 `locust-venv` Executor；在表单选择验证、搜索或生命周期模式。

平台依据最终退出码、审计、清理和文件完整性判断任务结果，支持取消后的补偿回收、报告下载与容量指标展示。使用 `storm/1.0` 报告结构，避免与普通 Locust 本地发送延迟混淆。详见同级平台仓库的 `docs/Colyseus-Storm闭环使用.md`。

旧 `colyseus-bot` / 直接 `locust -f locustfile.py` 保留作手动调试入口，需要显式设置与目标服务对应的本机 `STORM_DATABASE`，供账号归属核验和精确清理；未配置会失败。完整产品流程使用上述 `storm.py` 或平台 `colyseus-storm` 场景，由入口自动完成这些配置。

## 验证与实验记录

```powershell
& $py -m unittest discover -s tests -v
& $py -m unittest discover -s experiments -p 'test_*.py' -v
& $env:STORM_NODE experiments/test_step_statistics.cjs
& $env:STORM_NODE experiments/test_close_observer.cjs
```

既有性能实验已在指定同机环境验证 2,000 并发持续 300 秒，业务 RTT p95 139.27ms；该记录不是本次产品流程验收重新测得的容量。原始证据与工作负载见 [阶梯实验说明](experiments/STAIRCASE.md)；心跳修复对照见 [实验说明](experiments/README.md)。功能闭环验收采用低并发，不借此新增容量结论。

GitHub Actions 工作流按相同入口执行小规模功能验证并始终上传产物；远程 CI 结果以实际运行记录为准。

本次发布保留心跳对照实验的历史说明，不包含四个配套实验/分析脚本及本地原始报告。`test_heartbeat_ab.py` 依赖可选的历史实验脚本，缺少时会明确跳过；闭环与阶梯判定测试正常执行。
