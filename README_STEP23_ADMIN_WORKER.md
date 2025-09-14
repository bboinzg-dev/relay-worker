# Step 23 — Admin & Audit Console (Worker)

## DB 마이그레이션
- `db_migrations_step23_admin.sql`
  - `listings.status/blocked_reason/moderated_by/moderated_at`
  - `bids.status/blocked_reason/moderated_by/moderated_at`
  - `audit_logs` 테이블 + 범용 **row change trigger**(`audit_row_change`) 설치

## 엔드포인트(모두 admin)
- `GET  /api/admin/dashboard` — 개요 카운터 + 최근 감사로그 + 승인 대기 재고
- `GET  /api/admin/listings?status=pending|blocked|approved|all`
- `POST /api/admin/listings/:id/approve`
- `POST /api/admin/listings/:id/block` `{ reason }`
- `GET  /api/admin/bids?status=...`
- `POST /api/admin/bids/:id/block` `{ reason }`
- `GET  /api/admin/audit?table=&actor=&q=&limit=`

## 감사 로깅
- 트리거 기반: INSERT/UPDATE/DELETE 시 `audit_logs` 기록 (actor는 `current_setting('app.actor_id', true)`에서 읽음; 설정되지 않으면 NULL)
- 서버 액션 기반: `writeAudit()`로 명시적 기록 (헤더에서 actor/id/tenant 추출)

## 마운트
- `server.admin.js`를 `server.js`에 `app.use(require('./server.admin'))`로 연결
