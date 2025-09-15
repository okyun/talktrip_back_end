#!/bin/bash

echo "🚀 k6 성능 테스트 시작..."

# k6 컨테이너 실행
docker compose --profile testing up k6

echo "✅ k6 테스트 완료"
