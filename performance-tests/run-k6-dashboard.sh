#!/bin/bash

echo "🚀 k6 대시보드 시작..."

# InfluxDB와 Grafana 실행
echo "📊 InfluxDB와 Grafana 시작 중..."
docker compose --profile testing up -d influxdb grafana

echo "⏳ 서비스 시작 대기 중..."
sleep 10

# k6 테스트 실행 (InfluxDB로 데이터 전송)
echo "🧪 k6 테스트 실행 중..."
docker compose --profile testing up k6

echo "✅ k6 대시보드 설정 완료!"
echo "🌐 Grafana 대시보드: http://localhost:3000"
echo "   - 사용자명: admin"
echo "   - 비밀번호: admin123"
echo ""
echo "📊 InfluxDB: http://localhost:8086"
echo "   - 사용자명: admin"
echo "   - 비밀번호: admin123"
echo "   - 데이터베이스: k6"
