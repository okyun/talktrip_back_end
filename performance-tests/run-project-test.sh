#!/bin/bash

echo "🚀 TalkTrip 프로젝트 k6 테스트 시작..."

# 1. 애플리케이션 상태 확인
echo "📊 애플리케이션 상태 확인 중..."
if ! curl -s http://localhost/api/actuator/health > /dev/null; then
    echo "⚠️  애플리케이션이 응답하지 않습니다. 재시작 중..."
    docker compose restart talktrip-app-1 talktrip-app-2 talktrip-app-3
    echo "⏳ 애플리케이션 재시작 대기 중..."
    sleep 30
fi

# 2. InfluxDB와 Grafana 실행
echo "📈 모니터링 도구 시작 중..."
docker compose --profile testing up -d influxdb grafana

# 3. k6 테스트 실행
echo "🧪 k6 테스트 실행 중..."
docker compose --profile testing run --rm k6 run /scripts/talktrip-api-test.js

echo "✅ 테스트 완료!"
echo "🌐 Grafana 대시보드: http://localhost:3000"
echo "   - 사용자명: admin"
echo "   - 비밀번호: admin123"
