import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const options = {
    vus: 100,
    duration: '1m',
    thresholds: {
        http_req_failed: ['rate<0.01'],
        product_list_ms: ['p(95)<1200'],
    },
};

// 커스텀 메트릭 (키워드 검색만 검증)
const listDuration = new Trend('product_list_ms', true);
const listCount = new Counter('product_list_count');

export default function () {
    const base = __ENV.BASE_URL || 'http://localhost:8080';
    const keyword = __ENV.KEYWORD || '일본 힐링 여행';
    const country = encodeURIComponent(__ENV.COUNTRY || '전체');
    const size = __ENV.SIZE || '9';
    const page = __ENV.PAGE || '0';

    const listUrl = `${base}/api/products?countryName=${country}&size=${size}&page=${page}` + (keyword ? `&keyword=${encodeURIComponent(keyword)}` : '');

    const res = http.get(listUrl, {
        tags: { endpoint: 'GET /api/products' },
        timeout: '10s',
    });

    // 소요시간 기록
    listDuration.add(res.timings.duration);
    listCount.add(1);

    // 디버그 로그
    console.log(`list status=`, res.status);
    if (res.body) {
        console.log(`list body(200 bytes)=`, res.body.substring(0, 200));
    }

    check(res, { 'list status is 2xx': (r) => r.status >= 200 && r.status < 300 });

    // 0.1초 휴식 (사용자 think time 흉내)
    sleep(0.1);
}
