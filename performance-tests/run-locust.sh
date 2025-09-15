#!/bin/bash

echo "🚀 Locust 성능 테스트 시작..."

# Locust 컨테이너 실행
docker compose --profile testing up locust

echo "✅ Locust 테스트 완료"
echo "🌐 Locust 웹 UI: http://localhost:8089"
