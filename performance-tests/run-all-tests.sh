#!/bin/bash

echo "🚀 전체 성능 테스트 시작..."

# 1. k6 테스트 실행
echo "📊 k6 테스트 실행 중..."
docker compose --profile testing up k6

echo "⏳ 5초 대기..."
sleep 5

# 2. Locust 테스트 실행 (백그라운드)
echo "🦗 Locust 테스트 실행 중..."
docker compose --profile testing up -d locust

echo "✅ 모든 테스트가 시작되었습니다!"
echo "🌐 Locust 웹 UI: http://localhost:8089"
echo "📊 테스트 모니터링을 위해 위 URL을 확인하세요."
echo ""
echo "테스트를 중지하려면: docker compose --profile testing down"
