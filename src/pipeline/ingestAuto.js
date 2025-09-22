// src/pipeline/ingestAuto.js
'use strict';
/**
 * 개선된 ingestion 파이프라인(멀티 품번/표 우선).
 * - family_slug 지정 없으면 기존 휴리스틱으로 추정(기본 relay_power).
 * - component_registry→blueprint를 읽어, 추출 허용 컬럼을 확정.
 * - Document AI(있으면)→표 기반 추출, 없으면 pdf-parse 폴백.
 * - 표의 "Part Number/Ordering Code/Type/Model" 컬럼을 우선.
 * - 조합형 Ordering Information은 조합 폭이 과도하면 스킵.
 * - (brand_norm, code_norm) 유니크 키로 멱등 업서트.
 */
const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const db = require('../utils/db');
const { storage, parseGcsUri, readText, canonicalCoverPath } = require('../utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');

const { getBlueprint } = require('../utils/blueprint');
const { extractPartsAndSpecsFromPdf } = require('../ai/datasheetExtract');

// ---- helpers ---------------------------------------------------------------

function sanitizeId(s) {
  return String(s || '').toLowerCase().replace(/[^a-z0-9._-]/g, '-');
}

async function getTableColumns(qualifiedTable /* e.g. public.relay_power_specs */) {
  const [schema, table] = qualifiedTable.includes('.') ? qualifiedTable.split('.') : ['public', qualifiedTable];
  const q = `
    SELECT a.attname AS col
      FROM pg_attribute a
      JOIN pg_class c ON a.attrelid = c.oid
      JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1
       AND c.relname = $2
       AND a.attnum > 0
       AND NOT a.attisdropped
  `;
  const r = await db.query(q, [schema, table]);
  return new Set(r.rows.map(x => x.col));
}

async function getUpsertColumns(qualifiedTable) {
  const [schema, table] = qualifiedTable.includes('.') ? qualifiedTable.split('.') : ['public', qualifiedTable];
  const q = `
    SELECT array_agg(a.attname ORDER BY a.attnum) AS cols
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
     WHERE n.nspname = $1 AND c.relname = $2 AND i.indisunique`;
  const r = await db.query(q, [schema, table]);
  const first = r.rows[0]?.cols || [];
  const hasBrand = first.includes('brand_norm');
  const hasCode  = first.includes('code_norm');
  if (hasBrand && hasCode) return first;
  return ['brand_norm','code_norm'];
}

// Extract the largest image from page 1~2 using `pdfimages`, upload to GCS.
async function extractCoverToGcs(gcsPdfUri, { family, brand, code }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmpDir  = path.join(os.tmpdir(), 'pdf-' + Date.now());
    const pdfPath = path.join(tmpDir, 'doc.pdf');
    await fs.mkdir(tmpDir, { recursive: true });

    // Download PDF
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(pdfPath, buf);

    // Try to extract images from page 1~2; if pdfimages isn't installed, this throws.
    await execFileP('pdfimages', ['-f','1','-l','2','-png', pdfPath, path.join(tmpDir,'img')]);

    // Pick the largest PNG
    const files = (await fs.readdir(tmpDir)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!files.length) return null;
    let pick = null, size = -1;
    for (const f of files) {
      const st = await fs.stat(path.join(tmpDir, f));
      if (st.size > size) { pick = f; size = st.size; }
    }
    if (!pick) return null;

    const dst = canonicalCoverPath(
      (process.env.ASSET_BUCKET || process.env.GCS_BUCKET || '').replace(/^gs:\/\//,''),
      family, brand, code
    );
    const { bucket: outBkt, name: outName } = parseGcsUri(dst);
    await storage.bucket(outBkt).upload(path.join(tmpDir, pick), {
      destination: outName, resumable: false,
    });
    return dst;
  } catch (e) {
    return null; // best-effort
  }
}

function guessFamilySlug({ fileName = '', previewText = '' }) {
  const s = (fileName + ' ' + previewText).toLowerCase();
  if (/\b(resistor|r-clamp|ohm)\b/.test(s)) return 'resistor_chip';
  if (/\b(capacitor|mlcc|electrolytic|tantalum)\b/.test(s)) return 'capacitor_mlcc';
  if (/\b(inductor|choke)\b/.test(s)) return 'inductor_power';
  if (/\b(bridge|rectifier|diode)\b/.test(s)) return 'bridge_rectifier';
  if (/\b(relay|coil|omron|finder)\b/.test(s)) return 'relay_power';
  return null;
}

// ---- main ------------------------------------------------------------------

async function runAutoIngest({
  gcsUri,
  family_slug = null,
  brand = null,
  code = null,         // 유지: 명시 제공 시 단일 업서트
  series = null,
  display_name = null,
}) {
  const started = Date.now();
  if (!gcsUri) throw new Error('gcsUri required');

  // 0) Quick peek for family guess if not provided
  let fileName = '';
  try {
    const { name } = parseGcsUri(gcsUri);
    fileName = path.basename(name);
  } catch {}
  let family = (family_slug || '').toLowerCase() || guessFamilySlug({ fileName }) || 'relay_power';

  // Read a tiny piece of the document to help guessing (best-effort)
  if (!family) {
    try {
      const text = await readText(gcsUri, 256 * 1024);
      family = guessFamilySlug({ fileName, previewText: text }) || 'relay_power';
    } catch {
      family = 'relay_power';
    }
  }

  // 1) Resolve destination table from registry
  const reg = await db.query(
    `SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`,
    [family]
  );
  const table = reg.rows[0]?.specs_table || 'relay_power_specs';

  // 2) Ensure the table exists (DB 함수/블루프린트 기반 확장)
  await ensureSpecsTable(table, {
    datasheet_uri: 'text',
    image_uri: 'text',
    width_mm: 'numeric',
    height_mm: 'numeric',
    length_mm: 'numeric',
  });

  // 2.5) Blueprint 로딩 (허용 키 목록 획득)
  const { allowedKeys } = await getBlueprint(family);

  // 3) 멀티 품번 추출 (문서→표 우선)
  let extracted = { brand: brand || 'unknown', rows: [] };
  if (!brand || !code) {
    extracted = await extractPartsAndSpecsFromPdf({
      gcsUri,
      allowedKeys,
      brandHint: brand || null,
    });
  }

  // 4) 커버 이미지(첫 번째 품번 기준) - best effort
  let coverUri = null;
  try {
    const brandForCover = brand || extracted.brand || 'unknown';
    const codeForCover  = code  || extracted.rows?.[0]?.code || path.parse(fileName).name;
    coverUri = await extractCoverToGcs(gcsUri, {
      family, brand: brandForCover, code: codeForCover,
    });
  } catch {}

  // 5) 업서트 대상 레코드 구성
  const colsSet = await getTableColumns(table.startsWith('public.') ? table : `public.${table}`);

  const records = [];
  if (code) {
    // 단일 업서트 요청(브랜드/코드가 API 레벨에서 명시된 경우)
    records.push({
      family_slug: family,
      brand: brand || extracted.brand || 'unknown',
      code,
      series: series || null,
      display_name: display_name || null,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      updated_at: new Date(),
      verified_in_doc: false,
    });
  } else {
    // PDF에서 추출한 다수 품번
    for (const r of extracted.rows || []) {
      const base = {
        family_slug: family,
        brand: extracted.brand || 'unknown',
        code: r.code,
        datasheet_uri: gcsUri,
        image_uri: coverUri || null,
        display_name: `${extracted.brand || 'unknown'} ${r.code}`,
        verified_in_doc: !!r.verified_in_doc,
        updated_at: new Date(),
      };
      // 블루프린트 허용 키만 싹 넣기
      for (const k of allowedKeys) {
        if (r[k] != null) base[k] = r[k];
      }
      records.push(base);
    }
  }

  // 폴백: 아무것도 못 찾았으면 마지막에 한 건이라도 남기되 "TMP_xxxx"로 기록
  if (!records.length) {
    const tmpCode = 'TMP_' + (Math.random().toString(16).slice(2, 8)).toUpperCase();
    records.push({
      family_slug: family,
      brand: brand || extracted.brand || 'unknown',
      code: tmpCode,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      display_name: `${brand || extracted.brand || 'unknown'} ${tmpCode}`,
      verified_in_doc: false,
      updated_at: new Date(),
    });
  }

  // 6) 업서트
  let upserted = 0;
  for (const rec of records) {
    const safePayload = {};
    for (const [k, v] of Object.entries(rec)) {
      if (colsSet.has(k)) safePayload[k] = v;
    }
    await upsertByBrandCode(table, safePayload);
    upserted++;
  }

  return {
    ok: true,
    ms: Date.now() - started,
    family,
    specs_table: table,
    brand: records[0]?.brand,
    code: records[0]?.code,
    datasheet_uri: gcsUri,
    cover: coverUri,
    rows: upserted,
  };
}

module.exports = { runAutoIngest };
