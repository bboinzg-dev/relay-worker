// relay-worker/src/pipeline/ingestAuto.js
'use strict';

const db = require('../utils/db');
const { extractDataset } = require('../utils/extract'); // ← 아래 2번 파일에 구현됨

/**
 * 자동 인입 파이프라인
 * @param {Object} args
 * @param {string} args.gcsUri        gs://bucket/path.pdf
 * @param {string} [args.filename]    원본 파일명(브랜드/시리즈 힌트)
 * @param {string} [args.family_slug] 강제 패밀리 (없으면 브랜드로 추정)
 * @param {string} [args.brand]       브랜드 힌트
 * @param {string} [args.code]        코드 힌트
 * @param {string} [args.series]      시리즈 힌트
 * @param {string} [args.display_name]
 */
async function runAutoIngest({ gcsUri, filename, family_slug, brand, code, series, display_name }) {
  const t0 = Date.now();

  // 1) 데이터셋 추출 (DocAI → 부족 시 Vertex로 PDF 직접 읽기)
  const ds = await extractDataset({
    gcsUri,
    filename,
    maxInlinePages: +(process.env.MAX_DOC_PAGES_INLINE || 15),
    brandHint: brand,
    codeHint: code,
    seriesHint: series,
  });
  // ds => { brand, series, rows:[{code,series?,displayname?,verifiedPages?}], verifiedPages?, note? }

  // 2) 패밀리 결정 (없으면 브랜드 기반 규칙)
  const family = family_slug || guessFamilyByBrand(ds.brand) || 'relay_power';

  // 3) 스펙 테이블 보장
  await ensureRelayPowerTable();

  // 4) 업서트
  const rows = dedupeRows(ds.rows);
  let inserted = 0;
  for (const row of rows) {
    await db.query(
      `insert into public.relay_power_specs
        (brand, brand_norm, code, code_norm, family_slug, series, displayname, datasheet_uri, verified_in_doc, created_at)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
       on conflict (brand_norm, code_norm, family_slug) do nothing`,
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

  // 5) 로그 적재
  await db.query(
    `insert into public.doc_ingest_log
       (task, gcs_uri, filename, brand, family_slug, series_hint, page_count, rows, ms, note, created_at)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())`,
    [
      'relay_power',
      gcsUri,
      filename || null,
      ds.brand || null,
      family,
      ds.series || series || null,
      null, // page_count (필요시 추후 보강)
      inserted,
      Date.now() - t0,
      ds.note || null,
    ]
  );

  // 6) 호출자에게 요약 반환 (서버 로그/DB 패치가 이 구조를 기대)
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
  return (x || '').toLowerCase().trim() || null;
}

function guessFamilyByBrand(brand = '') {
  const b = brand.toLowerCase();
  if (!b) return 'relay_power';
  if (b.includes('panasonic')) return 'relay_power';
  if (b.includes('omron')) return 'relay_power';
  if (b.includes('te connectivity')) return 'relay_power';
  if (b.includes('finder')) return 'relay_power';
  if (b.includes('hongfa')) return 'relay_power';
  return 'relay_power';
}

async function ensureRelayPowerTable() {
  await db.query(`
    -- 테이블이 없으면 생성
    CREATE TABLE IF NOT EXISTS public.relay_power_specs (
      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
      brand           text,
      brand_norm      text,
      code            text,
      code_norm       text,
      family_slug     text,
      series          text,
      displayname     text,
      datasheet_uri   text,
      verified_in_doc jsonb DEFAULT '[]'::jsonb,
      created_at      timestamptz DEFAULT now()
    );

    -- 기존 테이블에도 누락 컬럼을 보강
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS brand           text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS brand_norm      text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS code            text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS code_norm       text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS family_slug     text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS series          text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS displayname     text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS datasheet_uri   text;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS verified_in_doc jsonb DEFAULT '[]'::jsonb;
    ALTER TABLE public.relay_power_specs ADD COLUMN IF NOT EXISTS created_at      timestamptz DEFAULT now();

    -- 중복 방지 인덱스
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = 'ux_rps_bcn'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX ux_rps_bcn ON public.relay_power_specs (brand_norm, code_norm, family_slug)';
      END IF;
    END $$;
  `);
}


module.exports = { runAutoIngest };
