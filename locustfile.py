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


class GamePlayerBot(HttpUser):
    wait_time = between(1, 3)
    host = "http://localhost:2567"

    def on_start(self):
        """注册账号 → 保存注册返回的 seat（signUp 内部已 matchmake）"""
        uid = random.randint(10000, 99999)
        self.email = f"locust_{uid}@test.com"
        self.password = "Test123456"
        self._seat = None
        self._login_cooldown = 0   # 登录冷却结束时间戳
        self._round = 0            # 第几轮

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

        ws_url = f"ws://localhost:2567/{process_id}/{room_id}?sessionId={session_id}"
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
                time.sleep(0.1)

            ws.close()
            print(f"[Round {self._round}] 游戏结束")

            # 登录后设置 30s 冷却（pendingSessionTimestamp 限制）
            if self._round >= 2:
                self._login_cooldown = time.time() + 30

        except Exception as e:
            print(f"[ERROR] Gameplay error: {type(e).__name__}: {e}")
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
