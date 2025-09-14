# Step 30 — 안정화 & E2E 스모크

## 포함물
- `db_seed_step30.sql` — 샘플 부품/재고/RFQ 시드
- `scripts/e2e-smoke.sh` — 업로드→검색→주문→커버렌더→품질 스캔 간단 점검

## 권장 체크리스트
1) **DDL 동결**: Step 18~29까지의 스키마가 모두 적용되었는지 확인
2) **시드 주입**: `db_seed_step30.sql` 실행
3) **E2E smoke**: 환경변수 `SITE` 지정 후 `scripts/e2e-smoke.sh` 실행
4) **모니터링**: event_queue 적체, 실패율, 결제 webhook 에러 로그 대시보드
5) **백업/복구 문서화**: Cloud SQL 자동 백업, GCS Lifecycle, 비상 복구 절차

## 다음(선택)
- Step 31: 인증/SSO + RLS
- Step 32: 검색 품질 v2
