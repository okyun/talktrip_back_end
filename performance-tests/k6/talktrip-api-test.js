import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// 커스텀 메트릭
const errorRate = new Rate('errors');
const successRate = new Rate('success');

// 테스트 설정
export const options = {
  stages: [
    { duration: '30s', target: 10 }, // 30초 동안 10명까지 증가
    { duration: '1m', target: 20 },  // 1분 동안 20명 유지
    { duration: '30s', target: 50 }, // 30초 동안 50명까지 증가
    { duration: '1m', target: 50 },  // 1분 동안 50명 유지
    { duration: '30s', target: 0 },  // 30초 동안 0명까지 감소
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'], // 95%의 요청이 2초 이내
    http_req_failed: ['rate<0.1'],     // 에러율 10% 미만
    errors: ['rate<0.1'],              // 커스텀 에러율 10% 미만
    success: ['rate>0.9'],             // 성공률 90% 이상
  },
  // InfluxDB 설정
  influxDB: {
    url: 'http://influxdb:8086',
    database: 'k6',
    username: 'admin',
    password: 'admin123',
  },
};

const BASE_URL = 'http://nginx';

export default function() {
  let response;
  let success = false;

  // 1. 헬스 체크
  response = http.get(`${BASE_URL}/api/actuator/health`);
  if (check(response, {
    'health check status is 200': (r) => r.status === 200,
  })) {
    success = true;
  }

  sleep(1);

  // 2. 채팅방 목록 조회
  response = http.get(`${BASE_URL}/api/chat/me/chatRooms/all`);
  if (check(response, {
    'chat rooms list accessible': (r) => r.status < 500,
  })) {
    success = true;
  }

  sleep(1);

  // 3. 특정 채팅방 메시지 조회
  const roomId = 'ROOM001';
  response = http.get(`${BASE_URL}/api/chat/rooms/${roomId}/messages?page=0&size=20`);
  if (check(response, {
    'chat messages accessible': (r) => r.status < 500,
  })) {
    success = true;
  }

  sleep(1);

  // 4. 채팅방 정보 조회
  response = http.get(`${BASE_URL}/api/chat/rooms/${roomId}`);
  if (check(response, {
    'room info accessible': (r) => r.status < 500,
  })) {
    success = true;
  }

  sleep(1);

  // 5. 메인 페이지 접근
  response = http.get(`${BASE_URL}/`);
  if (check(response, {
    'main page accessible': (r) => r.status < 500,
  })) {
    success = true;
  }

  // 메트릭 업데이트
  if (success) {
    successRate.add(1);
  } else {
    errorRate.add(1);
  }

  sleep(1);
}
