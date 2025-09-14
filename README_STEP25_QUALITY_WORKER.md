# Step 25 — Quality/Validation v1 (Worker)

## DB
- `db_migrations_step25_quality.sql`
  - `quality_rules` — per-family rule 세트(JSON)
  - `quality_runs` — 스캔 실행 로그
  - `quality_issues` — (Step19 확장) `suggestion_json`, `run_id`, `accepted_by`, `fixed_by` 등

## 엔드포인트 (admin)
- `POST /api/quality/run` `{ family? }` — 패밀리별 스캔 실행 → `quality_runs` + `quality_issues` 적재
- `GET  /api/quality/summary?family=` — completeness + open issue 카운트(패밀리별)
- `GET  /api/quality/issues?family=&status=&type=&severity=` — 이슈 조회
- `POST /api/quality/apply-suggested/:id` — 이슈의 `suggestion_json.fix` 적용(`fill_norms`/`fill_display_name`)
- `POST /api/quality/accept/:id` — Wontfix/accept 처리
- `POST /api/quality/fix` `{ family, op }` — 벌크 수정(`fill_norms` | `fill_display_name`)
- `GET/POST /api/quality/rules` — 룰 조회/갱신

## 스캐너 논리
- 필수 필드 누락(`missing`) — blueprint.required_fields + rules.required_fields
- 링크 누락(`link_missing`) — `datasheet_url`/`cover` 누락 개요
- 정규화(`normalization`) — brand_norm/code_norm NULL → `fill_norms` 제안
- 중복(`duplicate`) — (brand_norm, code_norm) 그룹 중복
- 이상치(`outlier`) — IQR 방식으로 수치 필드 탐지 (샘플만 suggestion에 포함)

## 사용 예
```bash
# 스캔 실행
curl -s -X POST "$SITE/api/proxy/quality/run" -H 'content-type: application/json' -H 'X-Actor-Roles: admin' -d '{"family":"relay"}' | jq .

# 요약
curl -s "$SITE/api/proxy/quality/summary?family=relay" -H 'X-Actor-Roles: admin' | jq .

# 이슈
curl -s "$SITE/api/proxy/quality/issues?family=relay&status=open" -H 'X-Actor-Roles: admin' | jq .

# 제안 적용(예: fill_norms)
curl -s -X POST "$SITE/api/proxy/quality/apply-suggested/<issueId>" -H 'X-Actor-Roles: admin' | jq .
```
