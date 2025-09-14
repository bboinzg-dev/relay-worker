# Step 26 — Ingest hooks + Thumbnail regen stub + Signed URL cache (Worker)

## DB
- `event_queue` — 비동기 이벤트 처리(queued/processing/done/error)
- `signed_url_cache` — GCS 서명 URL 캐시
- (옵션) `trg_enqueue_spec_relay` — `relay_specs` INSERT/UPDATE시 `spec_upsert` 이벤트 자동 생성

## 유틸
- `src/utils/gcsSignedUrl.js` — @google-cloud/storage V4 서명 URL + **메모리/DB 캐시**
- `src/utils/memcache.js` — 간단 TTL 캐시

## 엔드포인트
- `GET  /api/files/signed-url?gcs=gs://...&ttl=1200` — 서명 URL 발급(+캐시)
- `POST /api/hooks/spec-upsert` — 인제스트 파이프라인이 호출 → quality_scan/cover_regen/signed_url_warm 이벤트 큐잉
- `POST /api/tasks/process-events?limit=20` — 큐 처리기(Cloud Scheduler에서 1분마다 호출 권장)
- `GET  /api/tasks/queue` — 큐 상태 확인

## 이벤트 타입
- `spec_upsert` — family/brand/code 따라 하위 이벤트로 팬아웃
- `quality_scan_family` — Step 25 스캐너 실행(패밀리 단위)
- `cover_regen` — (스텁) cover 경로 보정/채움(실제 렌더 파이프라인은 후속 스텝에서)
- `signed_url_warm` — GCS 서명 URL 생성해서 캐시에 적재

## 마운트
```js
app.use(require('./server.files'));
app.use(require('./server.hooks'));
app.use(require('./server.tasks'));
```
