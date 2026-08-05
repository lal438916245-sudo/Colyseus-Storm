# ⚡ Colyseus-Storm 联机游戏底层协议级分布式压测引擎

**Colyseus-Storm** 是一款专为 Node.js (Colyseus) 联机游戏服务端打造的**轻量级无头分布式压测引擎**。不依赖重度游戏客户端（Unity），基于 Python gevent 协程与 Locust 框架，直接从 HTTP + WebSocket 协议层发起高并发请求，精确模拟真实玩家"注册 → 登录 → 匹配 → 连接 → 对局发包 → 退房"的完整生命周期。

---

## 🎯 核心技术

### 1. 底层二进制协议逆向封装

摒弃传统 JSON 文本协议，深入抓包并剖析 Colyseus 服务端底层通信模型。在 Python 中逆向重写了 Colyseus 的 `ROOM_DATA` 字节码封装规则：

```
[ROOM_DATA (1字节)] + [msgpack(消息类型)] + [msgpack(数据负载)]
```

利用 `msgpack` 二进制序列化替代 JSON，实现极低开销的高频状态同步（`entityUpdate`），单客户端发包 P50 < 1ms。

### 2. 无头压测架构

- **零 Unity 依赖**：直接调用 Colyseus RESTful 接口（`/users/signup`、`/users/login`）和原始 WebSocket 协议
- **Locust 分布式调度**：基于 gevent 协程，单进程轻松支撑 500+ 并发虚拟玩家
- **WebSocket 二进制通信**：绕过 Colyseus SDK，直接用 `websocket-client` 发送 msgpack 编码的二进制帧

### 3. 压测生命周期漏洞修复

在实战中发现并修复了以下关键问题：

| 问题 | 表现 | 修复方案 |
|------|------|----------|
| **Seat Reservation 过期** | 匹配后 15 秒未连接导致席位失效 | 注册后立即消费 seat；后续轮次通过 login 重新获取 seat |
| **onAuth 认证失败** | `matchmake` 拿到的 sessionId 与数据库 `pendingSessionId` 不匹配 | 第一轮使用 `signUp` 返回的 seat；后续轮次使用 `login` 返回的 seat |
| **登录 30s 冷却冲突** | `pendingSessionTimestamp` 未过期时重复登录被拒 | 引入 `_login_cooldown` 机制，精确控制登录间隔 |
| **Windows TCP 端口耗尽 (10053)** | 500 并发下 `ConnectionAbortedError` 导致 48% 连接失败 | WebSocket 连接重试机制（最多 3 次，间隔 0.5s），失败率降至 0% |

### 4. 容量评估与性能洞察

500 并发下的压测结论：

| 指标 | 数据 | 评价 |
|------|------|------|
| 发包吞吐量 | **998 RPS**（P50 < 1ms） | 极稳定，0% 失败 |
| WebSocket 连接 | P50 **2ms**，P99 **20ms** | 修复后 0% 失败 |
| 登录接口 | P50 1.4s（bcrypt 瓶颈） | 0.8% 失败 |
| 注册接口 | P50 2.2s（bcrypt + matchmake） | 0.4% 失败 |

---

## 🚀 功能和使用方法

### 环境要求

- Python 3.8+
- Locust 2.x
- Node.js Colyseus 游戏服务端 (`localhost:2567`)
- MongoDB（本地或 Atlas）
- `websocket-client`、`msgpack`

### 安装依赖

```bash
pip install locust websocket-client msgpack
```

### 连通性验证

正式压测前，先用 `bot_poc.py` 验证单用户能否完整走通全流程：

```bash
python bot_poc.py
```

脚本会依次执行：

1. **HTTP 匹配** — `POST /matchmake/joinOrCreate/lobby_room` 申请加入房间
2. **WebSocket 连接** — 用返回的 `roomId` + `sessionId` 建立长连接
3. **挂机 5 秒后退出**

成功输出示例：

```
[HTTP 握手成功] 获取到房间 ID: xxxxx, 分配的 Session ID: xxxxx
[连接成功] 虚拟玩家已成功潜入游戏房间！
虚拟玩家正在房间内挂机 (5秒)...
虚拟玩家已退出房间。
```

如果失败，说明服务端未启动或配置有误，先排查后再跑压测。

### 启动压测

#### 方式一：Web UI 模式（调试用）

```bash
locust -f locustfile.py
```

浏览器打开 `http://localhost:8089`，设置：
- **Number of users**: 并发虚拟玩家数（建议 100~500）
- **Spawn rate**: 孵化速率（建议 10~50）
- **Host**: `http://localhost:2567`

点击 **Start swarming** 开始压测。

#### 方式二：Headless 命令行模式（自动化测试用）

```bash
locust -f locustfile.py --headless -u 50 -r 10 --run-time 3m --csv=colyseus_test_run
```

| 参数 | 说明 |
|------|------|
| `--headless` | 无 UI 模式，直接运行 |
| `-u 50` | 并发用户数 |
| `-r 10` | 每秒孵化速率 |
| `--run-time 3m` | 运行时长（s=秒, m=分钟, h=小时） |
| `--csv=colyseus_test_run` | CSV 输出前缀，与 `analyze_locust.py` 读取的前缀保持一致 |

运行结束后自动生成 CSV 文件，直接执行 `python analyze_locust.py` 即可生成报告。

### 压测流程

```
on_start:
  注册账号 (/users/signup) → 保存 seatReservation
    ↓
Round 1 (task):
  使用注册 seat → WebSocket 连接 → 高频发送 entityUpdate (10~20s) → 断开
    ↓
Round 2+ (task):
  登录 (/users/login) 拿新 seat → WebSocket 连接 → 高频发送 entityUpdate → 断开
  → 等待 30s 冷却（服务端 pendingSession 限制）
    ↓
Round N...
```

### 性能打点

| 指标名称 | 含义 |
|----------|------|
| `[0] Login` | HTTP 登录接口耗时 |
| `[1] Connect` | WebSocket 连接耗时（含重试） |
| `[2] Send entityUpdate` | 二进制帧发送耗时（msgpack 编码 + socket send） |

### 关键参数

| 参数 | 位置 | 默认值 | 说明 |
|------|------|--------|------|
| `wait_time` | 类属性 | `between(1, 3)` | 两轮 task 之间的等待间隔 |
| `timeout` | `create_connection()` | `10` 秒 | WebSocket 连接超时 |
| 重试次数 | `for retry in range(3)` | `3` 次 | 连接失败重试次数 |
| 冷却时间 | `_login_cooldown` | `30` 秒 | 登录后等待 pendingSession 过期 |

---

### 数据分析

压测完成后，运行 `analyze_locust.py` 自动生成性能报告：

```bash
python analyze_locust.py
```

脚本会读取 Locust 生成的 CSV 文件，产出：

| 输出文件 | 路径 | 说明 |
|---------|------|------|
| CSV 报告 | `reports/colyseus_performance_report.csv` | 各接口汇总指标 + 稳定期聚合 + 失败分布 |
| 时序图表 | `reports/colyseus_performance_report.png` | RPS/失败率时序、用户数时序、P50/P95/P99 响应时间、RPS vs 用户散点图 |

**分析逻辑：**

- 剔除前 15% 爬坡期 + 后 5% 断连期，只分析稳定期数据
- 从 `_stats.csv` 提取各接口（HTTP/WebSocket）的汇总指标
- 从 `_stats_history.csv` 提取稳定期聚合指标（峰值吞吐量、P50/P95 等）
- 从 `_failures.csv` 提取失败分布

**数据覆盖机制：** 每次运行 Locust 和 `analyze_locust.py` 都会覆盖上一次的数据，不会残留旧结果。

---

## 🏗️ 技术栈

| 层级 | 技术 |
|------|------|
| 协程调度 | Python gevent + Locust |
| HTTP 客户端 | `requests` (Locust HttpUser) |
| WebSocket 客户端 | `websocket-client` |
| 序列化协议 | `msgpack` (二进制) |
| 游戏服务端 | Colyseus 0.14 + Node.js + TypeScript |
| 数据库 | MongoDB + MikroORM |
| 认证加密 | bcrypt (服务端) |
