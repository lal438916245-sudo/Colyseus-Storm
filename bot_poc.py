# bot_poc.py
import requests
import websocket
import json
import time

def simulate_single_bot():
    print("[虚拟玩家] 启动：正在尝试绕过客户端直接连接服务器...")
    
    # 1. HTTP 匹配阶段 (向服务器申请加入或创建大厅)
    matchmake_url = "http://localhost:2567/matchmake/joinOrCreate/lobby_room"
    try:
        response = requests.post(matchmake_url, json={}, timeout=5)
        response.raise_for_status()
        room_data = response.json()
        
        # 提取关键凭证
        room_id = room_data.get("room", {}).get("roomId") or room_data.get("roomId")
        session_id = room_data.get("sessionId")
        print(f"[HTTP 握手成功] 获取到房间 ID: {room_id}, 分配的 Session ID: {session_id}")
        
    except Exception as e:
        print(f"[HTTP 握手失败] 无法获取入场券: {e}")
        return

    # 2. WebSocket 长连接阶段 (伪造游戏底层通信协议)
    # Colyseus 的 WS 地址格式通常为: ws://localhost:2567/[room_name]/[room_id]?sessionId=[session_id]
    ws_url = f"ws://localhost:2567/lobby_room/{room_id}?sessionId={session_id}"
    
    print(f"[WebSocket] 正在发起 TCP 长连接: {ws_url}")
    
    try:
        # 建立长连接
        ws = websocket.create_connection(ws_url, timeout=5)
        print("[连接成功] 虚拟玩家已成功潜入游戏房间！")
        
        # 保持连接 5 秒
        print("虚拟玩家正在房间内挂机 (5秒)...")
        time.sleep(5)
        
        # 断开连接
        ws.close()
        print("虚拟玩家已退出房间。")
        
    except Exception as e:
        print(f"[WebSocket 连接失败] {e}")

if __name__ == "__main__":
    simulate_single_bot()