# 心跳读取对照实验

> 以下为本地历史实验记录。本次产品发布不包含 `heartbeat_ab.py`、`heartbeat_server.cjs`、`verify_heartbeat_results.py`、`overload_timeline.py` 及原始报告；历史实验命令需要另行取得这些材料。仓库中的心跳分类测试在缺少可选实验脚本时明确跳过。当前可直接运行的产品功能见根目录 README 与 STAIRCASE.md。

阶梯并发实验另见 [STAIRCASE.md](STAIRCASE.md)。

实验比较 `read_off`（进入观察阶段后只发不读）和 `read_on`（继续调用当前 `drain_inbound`）。使用真实 Colyseus MMO 服务端、真实注册接口和真实 `entityUpdate` 处理器，未替换业务实现。

## 已验证结果（2026-11-12）

正式采用 [20261112T073910Z_6a38fd86](../reports/heartbeat_ab/20261112T073910Z_6a38fd86/结论.md) 这一轮：两组各 30 条独立连接，每条目标观察 60 秒，每批 5 对、共 6 批，单连接每秒发送 10 次位置更新。

| 行为 | 样本数 | 60 秒达标 | 心跳超时 | 服务端收到的观察阶段 pong | 已生效的位置更新 |
|---|---:|---:|---:|---:|---:|
| `read_off` 只发不读 | 30 | 0/30 | 30/30 | 0 | 2,703 |
| `read_on` 现有读取修复 | 30 | 30/30 | 0/30 | 570 | 18,000 |

只发不读组的服务端终止时间为 **8.956–9.001 秒，中位数 8.991 秒**。每条持续读帧的连接均有 600 次实际业务更新；它们在 60 秒观察结束时仍在线，由实验主动清理。不能把这个观察上限解释为平均故障时间，也不能当作最大并发测试。

[逐次数据](../reports/heartbeat_ab/20261112T073910Z_6a38fd86/results.csv)、[原始日志审计](../reports/heartbeat_ab/20261112T073910Z_6a38fd86/audit.json) 和 [所有轮次的用途与排除原因](../reports/heartbeat_ab/结果索引.md) 已保存。正式轮次通过 1,131 项日志核验，60 个账号精确清理，专用服务退出码为 0。早期基础设施预检、探针排查轮次均未混入上述统计。

两组统一完成以下前置步骤：注册并取得关联账号的 seat、接收 `JOIN_ROOM(10)`、发送客户端确认、接收 `ROOM_STATE(14)`、服务端确认账号与一次位置更新生效、完成一轮 ping/pong。随后才开始 60 秒计时。除后续入站读取外，两组使用相同服务、发包频率和每对相同的随机负载序列。

## 运行

需要 Python 中已有 `websocket-client`、`requests`、`msgpack`、`pymongo`、`psutil`，以及原 MMO 服务端已经安装的 Node 依赖和本机 MongoDB。

```powershell
# 判定逻辑回归，不启动服务或创建账号
F:\Anaconda3\python.exe -m unittest discover -s experiments -p test_heartbeat_ab.py -v
F:\Colyseus_SDET_Project\Node\node.exe experiments\test_close_observer.cjs

# 小规模预检：一对连接，观察 15 秒
F:\Anaconda3\python.exe experiments\heartbeat_ab.py --pairs 1 --parallel-pairs 1 --duration 15

# 正式实验：每组 30 次、每次 60 秒，6 批，每批 5 对
F:\Anaconda3\python.exe experiments\heartbeat_ab.py --pairs 30 --parallel-pairs 5 --duration 60

# 完成后逐条核验客户端/服务端原始日志及输入快照，写入 audit.json
F:\Anaconda3\python.exe experiments\verify_heartbeat_results.py reports\heartbeat_ab\<本轮目录名>
```

默认原服务端目录为 `F:/Colyseus_SDET_Project/colyseus-unity-demo/colyseus-game-client/Server`，可用 `--server-root` 和 `--node` 改为实际位置。默认 MongoDB 为 `mongodb://127.0.0.1:27017`；脚本仅接受本机地址，不读取原服务端的 `arena.env`。使用新端口、自建服务进程和独立的 `heartbeat_ab_<run_id>` 数据库；结束后只删除本轮精确登记的 username/email，不操作原游戏数据库。

当前 `websocket-client 0.48` 在未显式设置代理主机时会丢弃 `http_no_proxy` 参数，因此实验使用预先连接的本机 TCP socket，避免受到环境代理影响。两组连接方式相同。`--trace` 可为诊断预检保存协议跟踪日志；正式实验不启用额外 trace。

## 证据与判定

每次输出独立目录 `reports/heartbeat_ab/<UTC时间_随机标识>/`：

- `manifest.json`：参数、机器信息、依赖版本、源文件与实际调用 helper 的 SHA256。
- `trials/*.jsonl`：HTTP 响应、准入帧、ping/pong、消息序列、异常和清理原始日志。
- `server-events.jsonl`：真实服务端 ping/pong、心跳计数、主动 terminate 和业务处理计数。
- `server-console.log`：服务端控制台输出，原 demo 打印的测试密码已屏蔽。
- `resource-samples.jsonl`：客户端和服务端的 CPU/RSS 采样。
- `results.csv`、`summary.json`、`结论.md`：逐次结果和汇总。
- `cleanup.json`：服务退出、精确账号清理和源文件未变校验。
- `audit.json`：离线交叉核验结论、原始证据及其他产物的 SHA256。
- `source-snapshot/`：实验依赖的脚本、真实服务端与 transport 输入快照，不包含 `arena.env`。

`heartbeat_timeout` 必须有成功的共同前置步骤、当前观察窗口内的服务端 terminate 证据，以及达到阈值的未应答心跳计数。握手、认证或其他基础设施失败归为无效/其他样本，不当成预期的心跳失败。`target_reached` 同时要求保持到规定时间、服务端仍在线且持续应用业务更新。

服务端默认每 3000ms 巡检、连续未应答计数达到 2 后在下一轮 terminate。起始相位不同会影响断开时点；本实验通过共同的一轮 ping/pong 对齐观察起点。`read_on` 成功只表示在规定窗口内达到目标，存活时间为右删失，不代表平均故障时间为 60 秒，也不是最大并发或容量结论。

原 `locustfile.py` 通过 AST 原样加载两个现有 helper，避免导入 Locust 触发全局 gevent monkey patch；生产脚本和服务端源码不修改。实验共同补齐了客户端 JOIN_ROOM 确认及服务端业务生效校验，因此结论限定为读取行为与心跳的受控对照，不能声称已验证原 Locust 脚本的所有业务细节。

`read_off` 模拟历史“只发不读”行为，`read_on` 使用当前修复 helper。这是同一代码中的读取开关对照，并非运行两个历史提交的完整版本回归。Colyseus 0.14 在正常离房时会按位置删除 close 监听器，探针通过观测事件派发采集关闭信息，不增加该监听器列表中的条目；`test_close_observer.cjs` 使用实际安装的框架离房方法验证此约束。
