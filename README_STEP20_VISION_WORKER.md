# Step 20 — Vision 파이프라인 v1

## Endpoints
- `POST /api/vision/index` (admin) — 이미지 업로드 또는 `gcsUri`를 받아 **임베딩 계산 → GCS 저장 → image_index 업서트**
  - form-data: `file` (binary) 또는 JSON: `{ gcsUri, family_slug?, brand?, code? }`
- `POST /api/vision/identify` (public) — 이미지/`gcsUri`로 최근접 후보 **Top-K** 조회, brand/code가 있으면 스펙 조인

## Env
- `GCS_BUCKET` — `gs://...` 형식 (버킷만 사용)
- `GOOGLE_CLOUD_PROJECT`, `VERTEX_LOCATION` — Vertex 사용 시 필요
- `VERTEX_EMBEDDING_MOCK=1` — 개발용 **모의 임베딩**(SHA-256 시드) 강제

## DB
- `image_index(embedding vector(1408))`, `pgvector` 확장 필요
- 마이그레이션: `db_migrations_step20_vision.sql`

## 확인
```bash
# index (관리자)
curl -s -X POST "$SITE/api/proxy/vision/index"     -H 'X-Actor-Roles: admin'     -F "file=@/path/to/relay.jpg"     -F "family_slug=relay" -F "brand=omron" -F "code=g2r-1a" | jq .

# identify (공개)
curl -s -X POST "$SITE/api/proxy/vision/identify"     -F "file=@/path/to/relay.jpg" | jq .
```
