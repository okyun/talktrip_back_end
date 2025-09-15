# 성능 테스트 도구

이 디렉토리는 TalkTrip 애플리케이션의 성능 테스트를 위한 k6와 Locust 설정을 포함합니다.

## 🚀 빠른 시작

### 1. 기본 애플리케이션 실행
```bash
# Docker Compose로 기본 서비스 실행
docker compose up -d
```

### 2. 성능 테스트 실행

#### k6 테스트 (CLI 기반)
```bash
# k6 테스트만 실행
./run-k6.sh

# 또는 직접 실행
docker compose --profile testing up k6
```

#### Locust 테스트 (웹 UI 기반)
```bash
# Locust 테스트 실행 (웹 UI 제공)
./run-locust.sh

# 또는 직접 실행
docker compose --profile testing up locust
```

#### 모든 테스트 실행
```bash
# k6 + Locust 동시 실행
./run-all-tests.sh
```

## 📊 테스트 도구

### k6
- **용도**: CLI 기반 성능 테스트
- **특징**: 빠른 실행, 상세한 메트릭, CI/CD 통합 용이
- **테스트 시나리오**: 
  - 헬스 체크
  - 채팅방 목록 조회
  - 채팅 메시지 조회
  - 채팅방 정보 조회

### Locust
- **용도**: 웹 UI 기반 성능 테스트
- **특징**: 실시간 모니터링, 사용자 친화적 UI
- **웹 UI**: http://localhost:8089
- **테스트 시나리오**:
  - 헬스 체크 (가장 자주 실행)
  - 채팅방 목록 조회
  - 채팅 메시지 조회
  - 채팅방 정보 조회

## ⚙️ 설정 커스터마이징

### k6 설정 수정
`k6/chat-load-test.js` 파일에서 다음을 수정할 수 있습니다:
- 테스트 단계 (stages)
- 임계값 (thresholds)
- 테스트 시나리오

### Locust 설정 수정
`locust/locustfile.py` 파일에서 다음을 수정할 수 있습니다:
- 사용자 행동 패턴 (tasks)
- 대기 시간 (wait_time)
- 테스트 시나리오

## 📈 모니터링

### k6 메트릭
- `http_req_duration`: HTTP 요청 지속 시간
- `http_req_failed`: HTTP 요청 실패율
- `errors`: 커스텀 에러율

### Locust 메트릭
- 웹 UI에서 실시간으로 확인 가능
- RPS (초당 요청 수)
- 응답 시간 분포
- 에러율

## 🛠️ 문제 해결

### 테스트가 실패하는 경우
1. 기본 애플리케이션이 실행 중인지 확인
2. 방 ID가 실제 존재하는지 확인 (`ROOM001` 등)
3. 네트워크 연결 상태 확인

### 컨테이너 정리
```bash
# 테스트 컨테이너만 중지
docker compose --profile testing down

# 모든 컨테이너 중지
docker compose down
```

## 📝 테스트 결과 해석

### 성공 기준
- **응답 시간**: 95%의 요청이 2초 이내
- **에러율**: 10% 미만
- **가용성**: 99% 이상

### 성능 개선 포인트
- 응답 시간이 임계값을 초과하는 엔드포인트
- 에러율이 높은 API
- 리소스 사용량이 높은 구간
