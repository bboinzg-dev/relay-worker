# Step 29 — 판매자 알림 & 입찰 자동화

## DB
- `subscriptions` — 판매자 구독(패밀리/테넌트별 이메일/웹훅)
- `notifications` — 발송 큐
- `purchase_requests`/`bids` 컬럼 보강(`family_slug`, `deadline`, `awarded_qty`, `score`)

## 서버
- `server.notify.js`
  - `GET/POST /api/subscriptions` (seller/admin)
  - `POST /api/notify/test`
- `server.tasks.notify.js`
  - 이벤트 처리: `rfq_created`, `bid_submitted`, `rfq_deadline_due`, `notify_deliver`
  - `/api/tasks/process-notify` — notifications와 event_queue를 함께 처리

## 연동 포인트
- RFQ 생성 시 `INSERT INTO event_queue(type,payload) VALUES ('rfq_created', {...})`
- 입찰 등록 시 `('bid_submitted', {...})`
- 마감 예약: Cloud Scheduler가 마감 시각에 `('rfq_deadline_due', { purchase_request_id })` enqueue

## 마운트
```js
app.use(require('./server.notify'));
app.use(require('./server.tasks.notify'));
```
