# Step 28 — Stripe 결제 & 재고 차감

## DB
- `db_migrations_step28_payments_stripe.sql` 적용 (payments 확장, listings 수량 체크)

## 서버
- `server.payments.stripe.js`
  - `POST /api/payments/stripe/session` — Checkout 세션 생성 → URL 반환
  - `POST /api/payments/stripe/webhook` — 결제 완료 처리(인보이스/주문 상태 전이 + **재고 차감**)
- `src/payments/fulfill.js` — `markInvoicePaid(invoice_id)` 트랜잭션 수행(수량 부족시 롤백)

## 환경변수
- `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET`
- (선택) `PAY_SUCCESS_URL`, `PAY_CANCEL_URL`

## 마운트
```js
app.use(require('./server.payments.stripe'));
```

## 흐름
1) `/api/payments/stripe/session` → session.url 리다이렉트
2) Stripe Checkout → 결제
3) Webhook 수신 `/api/payments/stripe/webhook` → `markInvoicePaid()` → **재고 감소**
