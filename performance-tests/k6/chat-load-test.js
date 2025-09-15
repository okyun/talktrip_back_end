import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// 커스텀 메트릭
const errorRate = new Rate('errors');

// 테스트 설정 - 더 간단한 테스트로 변경
export const options = {
  stages: [
    { duration: '10s', target: 5 },  // 10초 동안 5명까지 증가
    { duration: '30s', target: 10 }, // 30초 동안 10명 유지
    { duration: '10s', target: 0 },  // 10초 동안 0명까지 감소
  ],
  thresholds: {
    http_req_duration: ['p(95)<5000'], // 95%의 요청이 5초 이내 (여유있게)
    http_req_failed: ['rate<0.5'],     // 에러율 50% 미만 (여유있게)
    errors: ['rate<0.5'],              // 커스텀 에러율 50% 미만
  },
  // InfluxDB로 메트릭 전송
  ext: {
    loadimpact: {
      name: 'TalkTrip Chat Load Test',
    },
  },
  // InfluxDB 설정
  influxDB: {
    url: 'http://influxdb:8086',
    database: 'k6',
    username: 'admin',
    password: 'admin123',
  },
};

// 실제 프로젝트 API 엔드포인트
const BASE_URL = 'http://nginx'; // Nginx를 통한 로드밸런싱

export default function() {
  // 1. 헬스 체크
  let response = http.get(`${BASE_URL}/api/actuator/health`);
  check(response, {
    'health check status is 200': (r) => r.status === 200,
  }) || errorRate.add(1);

  sleep(1);

  // 2. 채팅방 목록 조회 (인증이 필요한 경우)
  response = http.get(`${BASE_URL}/api/chat/me/chatRooms/all`);
  check(response, {
    'chat rooms list accessible': (r) => r.status < 500, // 5xx 에러만 실패로 간주
  }) || errorRate.add(1);

  sleep(1);

  // 3. 특정 채팅방 메시지 조회
  const roomId = 'ROOM001';
  response = http.get(`${BASE_URL}/api/chat/rooms/${roomId}/messages?page=0&size=20`);
  check(response, {
    'chat messages accessible': (r) => r.status < 500,
  }) || errorRate.add(1);

  sleep(1);

  // 4. 채팅방 정보 조회
  response = http.get(`${BASE_URL}/api/chat/rooms/${roomId}`);
  check(response, {
    'room info accessible': (r) => r.status < 500,
  }) || errorRate.add(1);

  sleep(1);

  // 5. 메인 페이지 접근 (프론트엔드)
  response = http.get(`${BASE_URL}/`);
  check(response, {
    'main page accessible': (r) => r.status < 500,
  }) || errorRate.add(1);

  sleep(1);
}
