# Step 21 — 검색 UX 고도화 (필터·페이싯·정렬·그룹핑)

## Endpoint
- `GET /api/parts/searchx`
  - 쿼리:
    - `q` 텍스트
    - `page` (기본 1), `limit` (기본 24, 최대 100)
    - `sort`: `relevance`(기본) | `price_asc` | `price_desc` | `leadtime_asc` | `updated_desc`
    - 필터:
      - `family` (쉼표, 복수) — e.g. `family=relay,capacitor`
      - `brand` (복수)
      - `series` (복수)
      - `contact_form` (복수)
      - `coil_v_min`, `coil_v_max`
  - 응답:
    ```jsonc
    {
      "query": { ... },
      "total": 123,
      "items": [{
        "brand":"omron","code":"g2r-1a","display_name":"...","family_slug":"relay",
        "series":"G2R","contact_form":"1A","coil_voltage_vdc":24,
        "datasheet_url":"gs://.../datasheets/...pdf","cover":"gs://.../images/.../cover.png",
        "min_price_cents":2500,"total_qty":1200,"min_lead_days":3,
        "score":0.87
      }],
      "facets": {
        "brand":[{"key":"omron","count":10},...],
        "series":[...],
        "family":[...],
        "contact_form":[...],
        "coil_voltage_vdc":{"min":3,"max":48}
      }
    }
    ```

## 구현 메모
- 모든 등록 스펙 테이블(`component_registry`)을 **개별 쿼리** 후 메모리 머지/정렬/페이지네이션
- 유사도: `pg_trgm.similarity` 기반 간단 점수 + 가격/납기/업데이트 보조 정렬
- 가격/납기 집계: `listings`를 **서브쿼리 집계**로 LEFT JOIN
- 인덱스: 스펙 테이블에 trgm/series/family/contact_form/coil_vdc 인덱스 추가 (동봉 SQL)

## 배치
1) DB 마이그레이션: `db_migrations_step21_search.sql`
2) 서버 마운트: `server.search2.js`를 `server.js`에서 `app.use(require('./server.search2'))`
