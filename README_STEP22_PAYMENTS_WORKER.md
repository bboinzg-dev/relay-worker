# Step 22 — Payments/Settlement Stub (Orders & Invoices)

## DB (마이그레이션)
- `orders` / `order_items`
- `invoices`
- `payments` (provider session + 상태)
- 순번: `seq_order_no`, `seq_invoice_no`
> 파일: `db_migrations_step22_payments.sql`

## 엔드포인트
- `POST /api/checkout/preview` (buyer/admin)
  - 입력: `{ items:[{brand,code,qty}] }`
  - 출력: `plan.assignments[]` + `totals(subtotal/tax/shipping/total)` — **가장 싼 listing부터 할당**
- `POST /api/checkout/create` (buyer/admin)
  - 입력: `{ items:[...], notes? }` (또는 `plan`)
  - 동작: `orders` / `order_items` 생성 + `invoices` 1건 생성(상태 `unpaid`)
- `GET  /api/orders` (buyer 본인 또는 admin 전체)
- `GET  /api/orders/:id` (본인/관리자)
- `POST /api/payments/session` (buyer/admin)
  - 입력: `{ invoice_id, provider?='fakepg' }`
  - 출력: `{ redirect_url: '/pay/fake?sid=...' }` + `payments`행(`requires_action`)
- `POST /api/payments/fake/capture` (webhook 시뮬레이션)
  - 입력: `{ session_id }`
  - 동작: `payments.status='captured'` → `invoices.status='paid'` → `orders.status='paid'`

## 권한
- Step 18의 미들웨어 적용(구매 관련은 `buyer`/`admin`만)
