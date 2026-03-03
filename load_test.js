import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: 2,
      timeUnit: '1s',
      preAllocatedVUs: 30,
      maxVUs: 120,
      stages: [
        { target: 10, duration: '30s' },
        { target: 25, duration: '30s' },
        { target: 40, duration: '30s' },
        { target: 0, duration: '15s' },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.25'],
    http_req_duration: ['p(95)<1500'],
  },
};

const BASE = __ENV.BASE_URL || 'http://localhost:8003';
const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

function ids() {
  // Держим id в диапазоне int32, валидном для текущих моделей/БД.
  const suffix = (__VU * 1_000_000 + __ITER) % 400_000_000;
  return {
    userId: 1_000_000_000 + suffix,
    itemId: 1_500_000_000 + suffix,
    missingItemId: 2_000_000_000 + (suffix % 100_000_000),
  };
}

export default function () {
  const { userId, itemId, missingItemId } = ids();
  const isVerified = userId % 2 === 0;

  // 1) Health endpoint
  const rootRes = http.get(`${BASE}/`);
  check(rootRes, { 'GET / is 200': (r) => r.status === 200 });

  // 2) Create user
  const userPayload = JSON.stringify({
    id: userId,
    is_verified_seller: isVerified,
  });
  const userRes = http.post(`${BASE}/users`, userPayload, JSON_HEADERS);
  check(userRes, {
    'POST /users is 200 or 409': (r) => r.status === 200 || r.status === 409,
  });

  // 3) Create advertisement
  const adPayload = JSON.stringify({
    seller_id: userId,
    item_id: itemId,
    name: `Item-${itemId}`,
    description: `Description for item ${itemId}`,
    category: (itemId % 20) + 1,
    images_qty: itemId % 5,
  });
  const adRes = http.post(`${BASE}/advertisements`, adPayload, JSON_HEADERS);
  check(adRes, {
    'POST /advertisements is 200 or 409': (r) => r.status === 200 || r.status === 409,
  });

  // 4) Full predict
  const predictPayload = JSON.stringify({
    seller_id: userId,
    is_verified_seller: isVerified,
    item_id: itemId,
    name: `Item-${itemId}`,
    description: `Description for item ${itemId}`,
    category: (itemId % 20) + 1,
    images_qty: itemId % 5,
  });
  const predictRes = http.post(`${BASE}/predict`, predictPayload, JSON_HEADERS);
  check(predictRes, { 'POST /predict is 200': (r) => r.status === 200 });

  // 5) Cached/simple predict
  const simpleRes = http.get(`${BASE}/simple_predict?item_id=${itemId}`);
  check(simpleRes, { 'GET /simple_predict is 200': (r) => r.status === 200 });

  // 6) Async moderation + fetch task result
  const asyncPayload = JSON.stringify({ item_id: itemId });
  const asyncRes = http.post(`${BASE}/async_predict`, asyncPayload, JSON_HEADERS);
  check(asyncRes, {
    'POST /async_predict is 200': (r) => r.status === 200,
  });

  if (asyncRes.status === 200) {
    const body = asyncRes.json();
    if (body && body.task_id !== undefined) {
      const resultRes = http.get(`${BASE}/moderation_result/${body.task_id}`);
      check(resultRes, {
        'GET /moderation_result/{task_id} is 200': (r) => r.status === 200,
      });
    }
  }

  // 7) Intentional 404 for error-rate panel
  const missRes = http.get(`${BASE}/simple_predict?item_id=${missingItemId}`);
  check(missRes, {
    'GET /simple_predict miss is 404': (r) => r.status === 404,
  });

  sleep(0.1);
}
