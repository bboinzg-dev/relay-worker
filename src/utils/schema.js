/* relay-worker/src/utils/schema.js */
'use strict';

const db = require('./db');

const COMMON_COLS_SQL = `
  -- 공통 스펙 컬럼
  id bigserial primary key,
  family_slug text not null,
  brand text not null,
  code  text not null,
  brand_norm text generated always as (lower(brand)) stored,
  code_norm  text generated always as (lower(code)) stored,
  series text,
  display_name text,

  -- 크기(mm)
  width_mm  text,
  height_mm text,
  length_mm text,

  -- URL
  image_uri     text, -- 대표 이미지(GCS)
  datasheet_uri text, -- 데이터시트(GCS)

  -- 원본/추출
  source_gcs_uri text,
  raw_json jsonb,

  created_at timestamptz default now(),
  updated_at timestamptz default now()
`;

async function ensureSpecsTable(table, extraFields = {}) {
  const safe = String(table).replace(/[^a-zA-Z0-9_]/g, '');
  const createSql = `
    create table if not exists public.${safe} (
      ${COMMON_COLS_SQL}
    );
    do $$
    begin
      if not exists (select 1 from pg_indexes where schemaname='public' and indexname='${safe}_brand_code_idx') then
        execute 'create unique index ${safe}_brand_code_idx on public.${safe}(brand_norm, code_norm)';
      end if;
    end
    $$;
  `;
  await db.query(createSql);

  // 추가 family 전용 필드(있다면 텍스트로 생성)
  for (const k of Object.keys(extraFields || {})) {
    const col = k.replace(/[^a-zA-Z0-9_]/g, '');
    try {
      await db.query(`alter table public.${safe} add column if not exists ${col} text`);
    } catch {}
  }
  return safe;
}

async function upsertByBrandCode(table, row) {
  const safe = String(table).replace(/[^a-zA-Z0-9_]/g, '');
  const cols = [
    'family_slug','brand','code','series','display_name',
    'width_mm','height_mm','length_mm',
    'image_uri','datasheet_uri','source_gcs_uri','raw_json'
  ];
  const vals = cols.map(c => row[c] ?? null);

  const placeholders = vals.map((_,i)=>'$'+(i+1)).join(',');
  const updates = cols.map(c => `${c}=EXCLUDED.${c}`).join(', ');

  const sql = `
    insert into public.${safe} (${cols.join(', ')})
    values (${placeholders})
    on conflict (brand_norm, code_norm)
    do update set ${updates}, updated_at = now()
    returning *;
  `;
  const r = await db.query(sql, vals);
  return r.rows[0];
}

module.exports = { ensureSpecsTable, upsertByBrandCode };
