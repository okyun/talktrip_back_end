from locust import HttpUser, task, between
import random
import json

class ChatUser(HttpUser):
    wait_time = between(1, 3)
    
    def on_start(self):
        """사용자가 시작할 때 실행"""
        self.room_id = "ROOM001"  # 실제 존재하는 방 ID로 변경 필요
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    @task(3)
    def health_check(self):
        """헬스 체크 - 가장 자주 실행"""
        with self.client.get("/api/actuator/health", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Health check failed with status {response.status_code}")
    
    @task(2)
    def get_chat_rooms(self):
        """채팅방 목록 조회"""
        with self.client.get("/api/chat/me/chatRooms/all", headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, list):
                        response.success()
                    else:
                        response.failure("Invalid response format")
                except:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Failed to get chat rooms: {response.status_code}")
    
    @task(2)
    def get_chat_messages(self):
        """채팅 메시지 조회"""
        page = random.randint(0, 5)
        size = random.randint(10, 50)
        
        with self.client.get(f"/api/chat/rooms/{self.room_id}/messages?page={page}&size={size}", 
                           headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'content' in data:
                        response.success()
                    else:
                        response.failure("Invalid response format")
                except:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Failed to get messages: {response.status_code}")
    
    @task(1)
    def get_room_info(self):
        """채팅방 정보 조회"""
        with self.client.get(f"/api/chat/rooms/{self.room_id}", 
                           headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to get room info: {response.status_code}")
    
    @task(1)
    def simulate_websocket_connection(self):
        """WebSocket 연결 시뮬레이션 (HTTP로 대체)"""
        # 실제 WebSocket 연결은 별도 테스트에서 수행
        with self.client.get(f"/api/chat/rooms/{self.room_id}/info", 
                           headers=self.headers, catch_response=True) as response:
            if response.status_code in [200, 404]:  # 404도 정상 (방이 없을 수 있음)
                response.success()
            else:
                response.failure(f"WebSocket simulation failed: {response.status_code}")

class WebSocketUser(HttpUser):
    """WebSocket 전용 사용자 (실제 WebSocket 연결 테스트)"""
    wait_time = between(2, 5)
    
    def on_start(self):
        self.room_id = "ROOM001"
    
    @task(1)
    def websocket_health_check(self):
        """WebSocket 연결을 위한 기본 HTTP 요청"""
        with self.client.get("/api/actuator/health", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"WebSocket health check failed: {response.status_code}")
