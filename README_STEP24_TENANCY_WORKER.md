# Step 24 — Tenancy + Actor Propagation + Access Control (Worker)

## 핵심
- **DB 전역 함수** `public.fn_set_actor(actor_id, tenant_id, roles[])` — 트랜잭션 범위 세션 변수 설정
- 주요 테이블에 `tenant_id` 보강 + 인덱스
- 뷰 `v_listings_effective` — status='approved' & (tenant_id IS NULL OR tenant_id = current_setting('app.tenant_id'))
- 서버 엔드포인트 다수에 **tenant 범위 조건** 추가

## 마이그레이션
- 파일: `db_migrations_step24_tenancy.sql`

## 코드 변경
- 검색: `server.search2.tenancy.js` — listings 집계에서 `status='approved'` + tenant 범위
- 체크아웃/주문/결제: `server.checkout.tenancy.js`, `server.orders.tenancy.js`, `server.payments.tenancy.js`
  - `checkout/create`는 트랜잭션 시작 시 `SELECT fn_set_actor(...)` 호출(감사로그/정책 연동 대비)
- 어드민 카운터: `server.admin.tenancy.js` — `?tenant=` 스코프 파라미터 지원

## 주의
- 세션 변수(`app.tenant_id`)가 필요한 고급 정책은 **트랜잭션 내에서** `fn_set_actor`로 설정하세요.
- 이번 스텝은 **파라미터 기반 필터** 중심이라 풀 RLS는 도입하지 않았습니다(추후 스텝에서 RLS로 전환 가능).
