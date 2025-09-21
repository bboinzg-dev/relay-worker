'use strict';

const db = require('../utils/db');
const { extractDataset } = require('../utils/extract');

/**
 * 문서(PDF)에서 부품군/시리즈/코드들을 추출해 relay_power_specs에 UPSERT.
 * Cloud Tasks 없이 직접 호출해도 되고, Tasks 소비자에서 호출해도 됨.
 *
 * @param {Object} opts
 * @param {string} opts.gcsUri        gs://bucket/path.pdf
 * @param {string} [opts.filename]    원본 파일명(로그용)
 * @param {string} [opts.family_slug] 부품군(없으면 추정)
 * @param {string} [opts.brand]       제조사 힌트
 * @param {string} [opts.code]        품명(모델) 힌트
 * @param {string} [opts.series]      시리즈 힌트
 * @param {string} [opts.display_name]리스트용 표시명 힌트
 */
async function runAutoIngest({
  gcsUri,
  filename,
  family_slug,
  brand,
  code,
  series,
  display_name,
}) {
  if (!gcsUri || !gcsUri.startsWith('gs://')) {
    throw new Error('gcsUri is required (gs://bucket/object.pdf)');
  }

  const t0 = Date.now();

  // 1) 문서 → 구조화 추출 (DocAI/Vertex 내부 구현은 utils/extract에 캡슐화)
  const ds = await extractDataset({
    gcsUri,
    filename,
    maxInlinePages: +(process.env.MAX_DOC_PAGES_INLINE || 15),
    brandHint: brand,
    codeHint: code,
    seriesHint: series,
  });

  // 2) 패밀리 결정 (문서 내부 키워드 기반 폴백 추가)
  let family = family_slug || ds.family_slug || guessFamilyByBrand(ds.brand) || 'relay_power';
  const docText = (ds.raw_text || '').toLowerCase();
  if (!family_slug && !ds.family_slug) {
    if (docText.includes('signal relays') || /signal\s+relay/i.test(docText)) {
      family = 'relay_signal';
    }
  }

  // 3) 타깃 테이블 보장
  await ensureRelayPowerTable();

  // 4) 중복 제거 후 UPSERT
  const rows = dedupeRows(ds.rows);
  let upserts = 0;

  for (const r of rows) {
    await db.query(
      `
      insert into public.relay_power_specs
        (brand, brand_norm, code, code_norm, family_slug,
         series, displayname, datasheet_uri, verified_in_doc, created_at)
      values
        ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
      on conflict (brand_norm, code_norm, family_slug)
      do update set
        datasheet_uri   = excluded.datasheet_uri,
        series          = excluded.series,
        displayname     = excluded.displayname,
        verified_in_doc = excluded.verified_in_doc
      `,
      [
        ds.brand || null,
        norm(ds.brand),
        r.code,
        norm(r.code),
        family,
        r.series || ds.series || null,
        display_name || r.displayname || r.code,
        gcsUri,
        JSON.stringify(r.verifiedPages || ds.verifiedPages || []),
      ],
    );
    upserts++;
  }

  // 5) 인입 로그(트래킹)
  await ensureIngestLogTable();
  await db.query(
    `
    insert into public.doc_ingest_log
      (task, gcs_uri, filename, brand, family_slug, series_hint,
       page_count, rows, ms, note)
    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `,
    [
      'relay_power',
      gcsUri,
      filename || null,
      ds.brand || null,
      family,
      ds.series || series || null,
      null, // page_count (원하면 extractDataset에서 넘겨도 됨)
      upserts,
      Date.now() - t0,
      ds.note || null,
    ],
  );

  return {
    ok: true,
    brand: ds.brand || null,
    family,
    series: ds.series || null,
    rows: upserts,
    codes: rows.map((r) => r.code),
    datasheet_uri: gcsUri,
    specs_table: 'public.relay_power_specs',
    ms: Date.now() - t0,
  };
}

/* ------------------------- helpers ------------------------- */

function dedupeRows(rows = []) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const c = String(r?.code || '').trim();
    if (!c) continue;
    const key = c.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({ ...r, code: c });
  }
  return out;
}

function norm(x) {
  const s = (x || '').toLowerCase().trim();
  return s || null;
}

function guessFamilyByBrand(brand = '') {
  const b = (brand || '').toLowerCase();
  if (!b) return 'relay_power';
  if (b.includes('panasonic')) return 'relay_power';
  if (b.includes('omron')) return 'relay_power';
  if (b.includes('te connectivity') || b.includes('tyco')) return 'relay_power';
  if (b.includes('finder')) return 'relay_power';
  if (b.includes('hongfa')) return 'relay_power';
  return 'relay_power';
}

async function ensureRelayPowerTable() {
  await db.query(`
    create table if not exists public.relay_power_specs (
      id uuid default gen_random_uuid() primary key,
      brand text,
      brand_norm text,
      code text,
      code_norm text,
      family_slug text,
      series text,
      displayname text,
      datasheet_uri text,
      verified_in_doc jsonb default '[]'::jsonb,
      created_at timestamptz default now()
    );
    do $$
    begin
      if not exists (
        select 1 from pg_indexes
        where schemaname='public' and indexname='ux_rps_bcn'
      ) then
        execute
          'create unique index ux_rps_bcn
             on public.relay_power_specs (brand_norm, code_norm, family_slug)';
      end if;
    end $$;
  `);
}

async function ensureIngestLogTable() {
  await db.query(`
    create table if not exists public.doc_ingest_log (
      id uuid default gen_random_uuid() primary key,
      task text,
      gcs_uri text not null,
      filename text,
      brand text,
      family_slug text,
      series_hint text,
      page_count integer,
      rows integer,
      ms integer,
      note text,
      created_at timestamptz default now()
    );
  `);
}

module.exports = { runAutoIngest };
