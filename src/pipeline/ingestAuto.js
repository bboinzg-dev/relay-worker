// relay-worker/src/pipeline/ingestAuto.js
'use strict';

const db = require('../utils/db');
const { extractDataset } = require('../utils/extract');
const { normalizeFamilySlug } = require('../utils/family'); // 기존 함수 활용

async function runAutoIngest({ gcsUri, filename, family_slug }) {
  const t0 = Date.now();
  const ds = await extractDataset({ gcsUri, filename, maxInlinePages: +(process.env.MAX_DOC_PAGES_INLINE||15) });

  const family = family_slug || guessFamilyByBrand(ds.brand); // 없으면 규칙 기반 추정
  if (!family) throw new Error('Unable to determine family');

  // 테이블 보장(있다면 no-op)
  await ensureRelayPowerTable();

  let inserted = 0;
  for (const row of ds.rows) {
    await db.query(
      `insert into public.relay_power_specs (brand,brand_norm,code,code_norm,family_slug,series,displayname,datasheet_uri,verified_in_doc,created_at)
       values($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
       on conflict (brand_norm, code_norm, family_slug) do nothing`,
      [
        ds.brand, ds.brand.toLowerCase(), row.code, row.code.toLowerCase(),
        family, row.series || null,
        row.code, // display name
        gcsUri,   // datasheet_uri
        JSON.stringify(ds.verifiedPages || []),
      ]
    );
    inserted++;
  }

  await db.query(
    `insert into public.doc_ingest_log(task, gcs_uri, filename, brand, family_slug, series_hint, page_count, rows, ms, note, created_at)
     values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())`,
    [
      'relay_power', gcsUri, filename, ds.brand, family, ds.series,
      null, ds.rows.length, Date.now()-t0, ds.note || ''
    ]
  );

    return {
    ok: true,
    brand: ds.brand,
    family,
    series: ds.series,
    rows: ds.rows.length,
    codes: ds.rows.map(r => r.code),        // 새로 추가: 추출된 코드 목록
    datasheet_uri: gcsUri,                  // 새로 추가
    specs_table: 'public.relay_power_specs',// 새로 추가(본 파일에서 생성하는 테이블과 일치)
    ms: Date.now()-t0
  };
}

function guessFamilyByBrand(brand='') {
  const b = brand.toLowerCase();
  if (b.includes('panasonic')) return 'relay_power';
  if (b.includes('omron'))     return 'relay_power';
  return 'relay_power'; // 기본
}

async function ensureRelayPowerTable() {
  await db.query(`
    create table if not exists public.relay_power_specs (
      id uuid default gen_random_uuid() primary key,
      brand text, brand_norm text not null,
      code text, code_norm text not null,
      family_slug text not null,
      series text,
      displayname text,
      datasheet_uri text,
      verified_in_doc jsonb default '[]'::jsonb,
      created_at timestamptz default now()
    );
    create unique index if not exists ux_rps_bcn on public.relay_power_specs(brand_norm, code_norm, family_slug);
  `);
}

module.exports = { runAutoIngest };
