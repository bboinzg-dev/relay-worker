'use strict';

const db = require('../utils/db');
const { extractDataset } = require('../utils/extract');

/**
 * 자동 인제스트(데이터시트 → 스펙 DB)
 * @param {Object}  params
 * @param {string}  params.gcsUri        gs://... 원본 데이터시트 URI
 * @param {string=} params.filename      표시용 파일명
 * @param {string=} params.family_slug   부품군(미지정 시 추정)
 * @param {string=} params.brand         브랜드 힌트
 * @param {string=} params.code          모델명 힌트
 * @param {string=} params.series        시리즈 힌트
 * @param {string=} params.display_name  표기용 이름(없으면 code 사용)
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
  const t0 = Date.now();

  // 1) 데이터시트에서 메타/스펙 추출 (DocAI/Vertex 내부 util 사용)
  const ds = await extractDataset({
    gcsUri,
    filename,
    maxInlinePages: +(process.env.MAX_DOC_PAGES_INLINE || 15),
    brandHint: brand,
    codeHint: code,
    seriesHint: series,
  });

  // 2) 부품군 결정(입력 > 브랜드로 대략 추정 > 기본값)
  const family = family_slug || guessFamilyByBrand(ds.brand) || 'relay_power';

  // 3) 스펙 테이블 보장(없으면 생성)
  await ensureRelayPowerTable();

  // 4) 행 중복 제거 후 UPSERT
  const rows = dedupeRows(ds.rows);
  let inserted = 0;

  for (const row of rows) {
    await db.query(
      `
      insert into public.relay_power_specs
        (brand, brand_norm, code, code_norm, family_slug, series, displayname, datasheet_uri, verified_in_doc, created_at)
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
        row.code,
        norm(row.code),
        family,
        row.series || ds.series || null,
        display_name || row.displayname || row.code,
        gcsUri,
        JSON.stringify(row.verifiedPages || ds.verifiedPages || []),
      ]
    );
    inserted++;
  }

  // 5) 실행 로그 적재(없으면 테이블 생성)
  await db.query(`
    create table if not exists public.doc_ingest_log (
      id           uuid default gen_random_uuid() primary key,
      task         text,
      gcs_uri      text not null,
      filename     text,
      brand        text,
      family_slug  text,
      series_hint  text,
      page_count   integer,
      rows         integer,
      ms           integer,
      note         text,
      created_at   timestamptz default now()
    )
  `);

  await db.query(
    `
    insert into public.doc_ingest_log
      (task, gcs_uri, filename, brand, family_slug, series_hint, page_count, rows, ms, note)
    values
      ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `,
    [
      'relay_power',
      gcsUri,
      filename || null,
      ds.brand || null,
      family,
      ds.series || series || null,
      null,                 // page_count: 필요 시 extractDataset에서 넘겨서 기록
      inserted,
      Date.now() - t0,
      ds.note || null,
    ]
  );

  // 6) 응답
  return {
    ok: true,
    brand: ds.brand || null,
    family,
    series: ds.series || null,
    rows: inserted,
    codes: rows.map((r) => r.code),
    datasheet_uri: gcsUri,
    specs_table: 'public.relay_power_specs',
    ms: Date.now() - t0,
  };
}

/* ----------------------- 유틸 ----------------------- */

/** 행 중복 제거(코드 기준) */
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

/** 정규화: 소문자/trim, 빈문자열은 null */
function norm(x) {
  const s = (x || '').toLowerCase().trim();
  return s || null;
}

/** 브랜드로 대략적인 부품군 추정(기본은 전력 릴레이) */
function guessFamilyByBrand(brand = '') {
  const b = (brand || '').toLowerCase();
  if (!b) return 'relay_power';
  if (b.includes('panasonic'))        return 'relay_power';
  if (b.includes('omron'))            return 'relay_power';
  if (b.includes('te connectivity'))  return 'relay_power';
  if (b.includes('finder'))           return 'relay_power';
  if (b.includes('hongfa'))           return 'relay_power';
  return 'relay_power';
}

/** relay_power 스펙 테이블 보장 */
async function ensureRelayPowerTable() {
  await db.query(`
    create table if not exists public.relay_power_specs (
      id             uuid default gen_random_uuid() primary key,
      brand          text,
      brand_norm     text,
      code           text,
      code_norm      text,
      family_slug    text,
      series         text,
      displayname    text,
      datasheet_uri  text,
      verified_in_doc jsonb default '[]'::jsonb,
      created_at     timestamptz default now()
    );

    do $$
    begin
      if not exists (
        select 1
        from pg_indexes
        where schemaname='public' and indexname='ux_rps_bcn'
      ) then
        execute 'create unique index ux_rps_bcn on public.relay_power_specs (brand_norm, code_norm, family_slug)';
      end if;
    end $$;
  `);
}

module.exports = { runAutoIngest };
