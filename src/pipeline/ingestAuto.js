// src/pipeline/ingestAuto.js
'use strict';
/**
 * 전 패밀리 공용 인입 파이프라인(블루프린트 기반 멀티 품번/표 우선)
 */

const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const db = require('../utils/db');
const { storage, parseGcsUri, readText, canonicalCoverPath } = require('../utils/gcs');
const { upsertByBrandCode } = require('../utils/schema');

const { getBlueprint } = require('../utils/blueprint');
const { extractPartsAndSpecsFromPdf } = require('../ai/datasheetExtract');

const MAX_INLINE_TEXT = Number(process.env.MAX_DOC_PAGES_INLINE || 15);

/* -------------------- helpers -------------------- */
function norm(s){ return String(s||'').trim(); }
function normLower(s){ return norm(s).toLowerCase(); }

async function getTableColumns(qualified) {
  const [schema, table] = qualified.includes('.') ? qualified.split('.') : ['public', qualified];
  const q = `
    SELECT a.attname AS col
      FROM pg_attribute a
      JOIN pg_class c ON a.attrelid = c.oid
      JOIN pg_namespace n ON c.relnamespace = n.oid
     WHERE n.nspname = $1 AND c.relname = $2
       AND a.attnum > 0 AND NOT a.attisdropped`;
  const r = await db.query(q, [schema, table]);
  return new Set(r.rows.map(x=>x.col));
}

async function ensureSpecsTableByFamily(family){
  // 서버(플/pg) 함수가 블루프린트로 스키마 보장
  await db.query(`SELECT public.ensure_specs_table($1)`, [family]);
}

async function extractCoverToGcs(gcsPdfUri, { family, brand, code }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmp = path.join(os.tmpdir(), 'pdf-'+Date.now());
    const pdf = path.join(tmp, 'doc.pdf');
    await fs.mkdir(tmp, { recursive: true });
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(pdf, buf);
    await execFileP('pdfimages', ['-f','1','-l','2','-png', pdf, path.join(tmp,'img')]);

    const list = (await fs.readdir(tmp)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!list.length) return null;
    let pick=null, size=-1;
    for (const f of list) {
      const st = await fs.stat(path.join(tmp, f));
      if (st.size > size) { pick=f; size=st.size; }
    }
    if (!pick) return null;

    const dst = canonicalCoverPath((process.env.ASSET_BUCKET || process.env.GCS_BUCKET || '').replace(/^gs:\/\//,''), family, brand, code);
    const { bucket: outBkt, name: outName } = parseGcsUri(dst);
    await storage.bucket(outBkt).upload(path.join(tmp, pick), { destination: outName, resumable:false });
    return dst;
  } catch { return null; }
}

function guessFamilySlug({ fileName='', previewText='' }) {
  const s = (fileName+' '+previewText).toLowerCase();
  if (/\b(resistor|r-clamp|ohm)\b/.test(s)) return 'resistor_chip';
  if (/\b(capacitor|mlcc|electrolytic|tantalum)\b/.test(s)) return 'capacitor_mlcc';
  if (/\b(inductor|choke)\b/.test(s)) return 'inductor_power';
  if (/\b(bridge|rectifier|diode)\b/.test(s)) return 'bridge_rectifier';
  if (/\b(relay|coil|omron|finder)\b/.test(s)) return 'relay_power';
  return null;
}

/* -------------------- main -------------------- */
async function runAutoIngest({
  gcsUri, family_slug=null, brand=null, code=null, series=null, display_name=null,
}) {
  const started = Date.now();
  if (!gcsUri) throw new Error('gcsUri required');
  const { name: obj } = parseGcsUri(gcsUri);

  // family 추정
  let fileName = path.basename(obj);
  let family = (family_slug||'').toLowerCase() || guessFamilySlug({ fileName }) || 'relay_power';
  if (!family) {
    try {
      const text = await readText(gcsUri, 256*1024);
      family = guessFamilySlug({ fileName, previewText: text }) || 'relay_power';
    } catch { family = 'relay_power'; }
  }

  // 목적 테이블
  const reg = await db.query(
    `SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1`,
    [family]
  );
  const table = reg.rows[0]?.specs_table || 'relay_power_specs';
  const qualified = table.startsWith('public.')? table : `public.${table}`;

  // 스키마 보장(DB 함수)
  await ensureSpecsTableByFamily(family);
  const colsSet = await getTableColumns(qualified);

  // 블루프린트 허용 키
  const { allowedKeys } = await getBlueprint(family);

  // PDF → 품번/스펙 추출(전 패밀리 공용)
  let extracted = { brand: brand || 'unknown', rows: [] };
  if (!brand || !code) {
    extracted = await extractPartsAndSpecsFromPdf({
      gcsUri, allowedKeys, brandHint: brand || null,
    });
  }

  // 커버 이미지 (첫 품번 기준, best effort)
  let coverUri = null;
  try {
    const bForCover = brand || extracted.brand || 'unknown';
    const cForCover = code || extracted.rows?.[0]?.code || path.parse(fileName).name;
    coverUri = await extractCoverToGcs(gcsUri, { family, brand: bForCover, code: cForCover });
  } catch {}

  // 레코드 구성
  const records = [];
  const now = new Date();

  if (code) {
    // 단일 강제 인입(호출자가 brand/code를 준 경우)
    records.push({
      family_slug: family,
      brand: brand || extracted.brand || 'unknown',
      code,
      series: series || null,
      display_name: display_name || null,
      datasheet_uri: gcsUri,
      image_uri: coverUri || null,
      verified_in_doc: false,
      updated_at: now,
    });
  } else {
    for (const r of (extracted.rows || [])) {
      const base = {
        family_slug: family,
        brand: extracted.brand || 'unknown',
        code: r.code,
        datasheet_uri: gcsUri,
        image_uri: coverUri || null,
        display_name: `${extracted.brand || 'unknown'} ${r.code}`,
        verified_in_doc: true,
        updated_at: now,
      };
      // 블루프린트 허용 값만 추가
      for (const k of allowedKeys) { if (r[k] != null) base[k] = r[k]; }
      records.push(base);
    }
  }

  // 최후 폴백 줄이기: DocAI와 pdf-parse가 둘 다 실패했어도,
  // 자유 텍스트에서 code를 1개라도 잡았다면 그걸 쓰므로 TMP로 거의 안 떨어짐.
  if (!records.length) {
    const tmp = 'TMP_' + (Math.random().toString(16).slice(2, 8)).toUpperCase();
    records.push({
      family_slug: family, brand: brand || extracted.brand || 'unknown',
      code: tmp, datasheet_uri: gcsUri, image_uri: coverUri || null,
      display_name: `${brand || extracted.brand || 'unknown'} ${tmp}`,
      verified_in_doc: false, updated_at: now,
    });
  }

  // 업서트
  let upserted = 0;
  for (const rec of records) {
    const safe = {};
    // 공통 키
    if (colsSet.has('family_slug')) safe.family_slug = rec.family_slug;
    if (colsSet.has('brand'))       safe.brand = rec.brand;
    if (colsSet.has('code'))        safe.code = rec.code;
    if (colsSet.has('brand_norm'))  safe.brand_norm = normLower(rec.brand);
    if (colsSet.has('code_norm'))   safe.code_norm  = normLower(rec.code);
    if (colsSet.has('datasheet_uri')) safe.datasheet_uri = rec.datasheet_uri;
    if (colsSet.has('image_uri'))     safe.image_uri     = rec.image_uri;
    if (colsSet.has('datasheet_url')) safe.datasheet_url = rec.datasheet_uri; // 별칭 호환
    if (colsSet.has('display_name'))  safe.display_name  = rec.display_name;
    if (colsSet.has('displayname'))   safe.displayname   = rec.display_name;
    if (colsSet.has('cover') && rec.image_uri) safe.cover = rec.image_uri;
    if (colsSet.has('verified_in_doc')) safe.verified_in_doc = !!rec.verified_in_doc;

    // 블루프린트 value들
    for (const [k,v] of Object.entries(rec)) {
      if (['family_slug','brand','code','brand_norm','code_norm','datasheet_uri','image_uri','datasheet_url','display_name','displayname','cover','verified_in_doc','updated_at'].includes(k)) continue;
      if (colsSet.has(k)) safe[k] = v;
    }
    if (colsSet.has('updated_at')) safe.updated_at = now;

    await upsertByBrandCode(table, safe);
    upserted++;
  }

  return {
    ok: true,
    ms: Date.now() - started,
    family,
    specs_table: table,
    brand: records[0]?.brand,
    code:  records[0]?.code,
    datasheet_uri: gcsUri,
    cover: records[0]?.image_uri || null,
    rows: upserted,
  };
}

module.exports = { runAutoIngest };
