import time
import random
import json
import websocket
import msgpack
from locust import HttpUser, task, between, events

# Colyseus 的二进制操作码-"自定义消息"
ROOM_DATA = 13

def encode_colyseus_message(message_type, payload):
    """
    将消息编码为 Colyseus 的二进制格式
    Colyseus 协议统一用 msgpack 做序列化：
    比 JSON 体积小、序列化速度快，非常适合游戏实时通信。
    """
    buffer = bytearray([ROOM_DATA])
    buffer.extend(msgpack.packb(message_type))
    buffer.extend(msgpack.packb(payload))
    return bytes(buffer)


# 单轮内单次排空的帧数上限：防止服务端持续高速推送时排空循环长时间占用协程。
# 若单次达到上限，剩余帧会在下一轮循环继续被消费，不会丢失。
DRAIN_MAX_FRAMES = 256


def drain_inbound(ws):
    """非阻塞排空 WebSocket 入站帧，返回 (帧数, 字节数)。

    为什么必须读：Colyseus 的 WebSocket 传输层会按 pingInterval（默认 3000ms）
    向客户端发送 **WS 控制帧 ping**，并用 pingCount 记连续未响应次数 —— 一旦
    pingCount 达到 pingMaxRetries（默认 2）就主动 terminate() 该连接。
    三个周期叠加约 9s，与实测吻合：改造前「只发不读」的连接 9.15s 后下一次
    send 即抛 WinError 10053。关键在于 **pong 只在客户端读取入站数据时才被
    应答**（websocket-client 在 recv 内部自动回 pong），服务端收到 pong 才会
    把 pingCount 归零。

    实测本房间并不会持续下发状态补丁（6s 窗口内入站仅 1 帧 / 827B 的 JOIN
    全量状态），所以排空的主要收益是「维持心跳」，而不是「清空缓冲积压」。

    安全性：websocket-client 的 frame_buffer 跨调用保留「半帧」状态（残量留在
    recv_buffer，header/length 只在整帧组装完成时才 clear），因此某次 recv 因
    无数据而中断后，下次调用可无缝续读，不会破坏帧边界。
    """
    frames = 0
    nbytes = 0
    prev_timeout = ws.gettimeout()
    ws.settimeout(0)  # 非阻塞：无数据时抛 BlockingIOError，不会阻塞
    try:
        while frames < DRAIN_MAX_FRAMES:
            try:
                frame = ws.recv()
            except (BlockingIOError, websocket.WebSocketTimeoutException):
                break  # 接收缓冲已排空
            except websocket.WebSocketConnectionClosedException:
                break  # 连接已断，交给上层 send/close 报错
            frames += 1
            if isinstance(frame, (bytes, bytearray)):
                nbytes += len(frame)
    finally:
        try:
            ws.settimeout(prev_timeout)  # 恢复阻塞语义，不影响后续 send
        except Exception:
            pass
    return frames, nbytes


class GamePlayerBot(HttpUser):
    wait_time = between(1, 3)
    # 仅作缺省值：运行时会被 --host 覆盖（locust runners.py: `user_class.host = environment.host`）。
    # 平台执行时会传入 `--host http://{Environment.host}:{Environment.port}`，
    # 因此真正的地址来自平台的 Environment 配置实体，而不是这里。
    host = "http://localhost:2567"

    def _ws_base(self) -> str:
        """由 locust **实际生效**的 HTTP host 推导 WebSocket 基址。

        为什么需要它：`self.client`（REST 部分）本来就跟着 `--host` 走，但 WS 地址
        此前被写死成 `ws://localhost:2567/...` —— 于是服务端不在本机 2567 时，
        REST 打对了地址、WS 却连错地方，结果不可信。

        Colyseus 的 WebSocket 与 HTTP 同源同端口，故可直接换算：
          http://host:port  -> ws://host:port
          https://host:port -> wss://host:port
        这样「改平台 Environment 配置 → HTTP 与 WS 一起变」，脚本无需读环境变量、
        平台也无需新增配置项。若将来 WS 与 HTTP 真的不同源（反代/分离部署），
        再引入独立配置字段即可。
        """
        base = (self.host or "http://localhost:2567").rstrip("/")
        if base.startswith("https://"):
            return "wss://" + base[len("https://"):]
        if base.startswith("http://"):
            return "ws://" + base[len("http://"):]
        return "ws://" + base

    def on_start(self):
        """注册账号 → 保存注册返回的 seat（signUp 内部已 matchmake）"""
        uid = random.randint(10000, 99999)
        self.email = f"locust_{uid}@test.com"
        self.password = "Test123456"
        self._seat = None
        self._login_cooldown = 0   # 登录冷却结束时间戳
        self._round = 0            # 第几轮
        self._drained_frames = 0   # 累计排空的入站帧数（诊断用）
        self._drained_bytes = 0    # 累计排空的入站字节数（诊断用）

        with self.client.post("/users/signup", json={
            "username": f"locust_{uid}",
            "email": self.email,
            "password": self.password,
        }, catch_response=True) as resp:
            if resp.status_code == 200:
                data = resp.json()
                output = data.get("output", {})
                seat = output.get("seatReservation", {})
                room = seat.get("room", {})
                self._seat = {
                    "roomId": room.get("roomId"),
                    "processId": room.get("processId"),
                    "sessionId": seat.get("sessionId"),
                }
                resp.success()
            else:
                resp.failure(f"注册失败: {resp.text}")

    @task
    def play_game(self):
        self._round += 1

        # --- 第1轮：用注册返回的 seat ---
        if self._round == 1:
            if not self._seat:
                return
            print(f"[Round 1] 使用注册 seat: {self._seat['sessionId']}")

        # --- 第2轮起：用登录拿新 seat ---
        else:
            # 等待登录冷却结束
            wait = self._login_cooldown - time.time()
            if wait > 0:
                print(f"[Round {self._round}] 等待登录冷却 {wait:.1f}s...")
                time.sleep(wait)

            seat = self._login()
            if not seat:
                return
            self._seat = seat
            print(f"[Round {self._round}] 使用登录 seat: {seat['sessionId']}")

        # --- 连接 WebSocket + 发包 ---
        room_id = self._seat["roomId"]
        process_id = self._seat["processId"]
        session_id = self._seat["sessionId"]

        # WS 地址由 locust 实际生效的 host 推导（平台 Environment 配置 -> --host），
        # 不再写死 localhost:2567，避免"REST 打对了、WS 连错地方"。
        ws_url = f"{self._ws_base()}/{process_id}/{room_id}?sessionId={session_id}"
        start_time = time.time()

        # 连接重试（最多3次，每次间隔0.5s），应对 10053 端口耗尽
        ws = None
        last_error = None
        for retry in range(3):
            try:
                ws = websocket.create_connection(ws_url, timeout=10)
                break
            except Exception as e:
                last_error = e
                if retry < 2:
                    time.sleep(0.5)

        if ws is None:
            connect_time = int((time.time() - start_time) * 1000)
            events.request.fire(
                request_type="WebSocket",
                name="[1] Connect",
                response_time=connect_time,
                response_length=0,
                exception=last_error
            )
            print(f"[ERROR] Connect failed after 3 retries: {last_error}")
            if self._round >= 2:
                self._login_cooldown = time.time() + 30
            return

        connect_time = int((time.time() - start_time) * 1000)
        events.request.fire(
            request_type="WebSocket",
            name="[1] Connect",
            response_time=connect_time,
            response_length=0,
            exception=None
        )

        # 本轮统计（放在 try 之前，保证异常分支也能引用）
        round_start = time.time()   # 本轮 WebSocket 已连接后的起始时刻
        round_frames = 0            # 本轮排空的帧数
        round_bytes = 0             # 本轮排空的字节数
        play_duration = 0           # 本轮计划对局时长（秒），下面覆盖

        try:

            play_duration = random.randint(10, 20)
            game_end_time = time.time() + play_duration

            while time.time() < game_end_time:
                send_start = time.time()
                entity_update = [
                    session_id,
                    "xPos", round(random.uniform(-100, 100), 2),
                    "yPos", 0,
                    "zPos", round(random.uniform(-100, 100), 2),
                ]
                binary_data = encode_colyseus_message("entityUpdate", entity_update)
                ws.send(binary_data, opcode=websocket.ABNF.OPCODE_BINARY)
                send_cost = int((time.time() - send_start) * 1000)
                events.request.fire(
                    request_type="WebSocket",
                    name="[2] Send entityUpdate",
                    response_time=send_cost,
                    response_length=len(binary_data),
                    exception=None
                )
                # 排空入站帧：读取入站数据时 websocket-client 会自动应答服务端
                # 的 ping，使服务端 pingCount 归零；不读则约 9s 后被 terminate()。
                frames, nbytes = drain_inbound(ws)
                round_frames += frames
                round_bytes += nbytes
                time.sleep(0.1)

            round_elapsed = time.time() - round_start
            ws.close()
            self._drained_frames += round_frames
            self._drained_bytes += round_bytes
            print(f"[Round {self._round}] 游戏结束"
                  f"（对局 {round_elapsed:.1f}s / 计划 {play_duration}s，"
                  f"排空入站 {round_frames} 帧 / {round_bytes} 字节）")

            # 登录后设置 30s 冷却（pendingSessionTimestamp 限制）
            if self._round >= 2:
                self._login_cooldown = time.time() + 30

        except Exception as e:
            round_elapsed = time.time() - round_start
            print(f"[ERROR] Gameplay error: {type(e).__name__}: {e}"
                  f"（对局已持续 {round_elapsed:.1f}s / 计划 {play_duration}s，"
                  f"此前排空入站 {round_frames} 帧）")
            # 把对局异常接进 Locust 统计：原先只 print，导致连接被中止一事
            # 被「0 失败」掩盖，看不出真实问题。
            events.request.fire(
                request_type="WebSocket",
                name="[3] Gameplay round",
                response_time=int(round_elapsed * 1000),
                response_length=0,
                exception=e
            )
            self._drained_frames += round_frames
            self._drained_bytes += round_bytes
            try:
                ws.close()
            except:
                pass
            if self._round >= 2:
                self._login_cooldown = time.time() + 30

    def _login(self):
        """登录拿 seatReservation"""
        try:
            with self.client.post("/users/login", json={
                "email": self.email,
                "password": self.password,
            }, catch_response=True) as response:
                if response.status_code != 200:
                    response.failure(f"登录失败: {response.status_code}")
                    return None

                data = response.json()
                output = data.get("output", {})
                seat = output.get("seatReservation", {})
                room = seat.get("room", {})

                result = {
                    "roomId": room.get("roomId"),
                    "processId": room.get("processId"),
                    "sessionId": seat.get("sessionId"),
                }

                if not all(result.values()):
                    response.failure("登录返回数据不完整")
                    return None

                response.success()
                return result
        except Exception as e:
            events.request.fire(
                request_type="HTTP",
                name="[0] Login",
                response_time=0,
                response_length=0,
                exception=e
            )
            return None
