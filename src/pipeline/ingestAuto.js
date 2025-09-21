// src/pipeline/ingestAuto.js
// 모든 부품군 공용 ingestion (기존 기능 보존)
// - family_slug 명시 시 그대로 사용
// - 없으면 휴리스틱(필요 시 Vertex)으로 판별
// - component_registry -> specs_table 조회 + ensure_specs_table(family) 호출
// - 대표 이미지(cover) 추출(pdfimages) 시도, 실패해도 무시
// - 테이블 실제 컬럼 목록을 조회해 존재하는 컬럼만 UPSERT
'use strict';

const path = require('node:path');
const fs = require('node:fs/promises');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const db = require('../utils/db');
const { chooseCanonicalFamilySlug } = require('../utils/family');
const { storage, parseGcsUri, canonicalCoverPath } = require('../utils/gcs');

let VertexAI;
try { VertexAI = require('@google-cloud/vertexai').VertexAI; } catch (_) {}
const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCLOUD_PROJECT;
const LOCATION   = process.env.VERTEX_LOCATION || 'asia-northeast3';
const MODEL_CLS  = process.env.GEMINI_MODEL_CLASSIFY || 'gemini-2.5-flash';

// ---------- helpers ----------
const lc = s => (s || '').toString().trim().toLowerCase();
const now = () => new Date();

async function decideFamily({ explicit, text }) {
  if (explicit) return lc(explicit);

  // 1) 휴리스틱
  const byHeur = chooseCanonicalFamilySlug({ text }) || null;
  if (byHeur) return byHeur;

  // 2) Vertex (가능 시만)
  if (!VertexAI || !PROJECT_ID) return 'unknown';
  try {
    const vertex = new VertexAI({ project: PROJECT_ID, location: LOCATION });
    const model  = vertex.getGenerativeModel({ model: MODEL_CLS });
    const reg    = await db.query(`SELECT family_slug FROM public.component_registry ORDER BY family_slug`);
    const choices = reg.rows.map(r => r.family_slug).join(', ');
    const prompt = [
      'Classify the electronic component family from this datasheet text.',
      `Return ONLY one slug from this list (else return "unknown"): ${choices}`
    ].join('\n');
    const resp = await model.generateContent({
      contents: [{ role: 'user', parts: [{ text: prompt }, { text }]}]
    });
    const out  = (resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '').trim();
    const slug = out.split(/\s+/)[0].toLowerCase().replace(/[^a-z0-9_]/g,'');
    return slug || 'unknown';
  } catch { return 'unknown'; }
}

// 해당 테이블의 실제 컬럼 목록
async function getTableColumns(qualifiedTable) {
  const [schema, table] = qualifiedTable.split('.');
  const q = `
    SELECT column_name
      FROM information_schema.columns
     WHERE table_schema=$1 AND table_name=$2`;
  const r = await db.query(q, [schema, table]);
  return new Set(r.rows.map(row => row.column_name));
}

// UPSERT에 사용할 유니크 충돌 컬럼 추출
async function getConflictColumns(qualifiedTable) {
  const [schema, table] = qualifiedTable.split('.');
  const q = `
  SELECT tc.constraint_name,
         array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS cols
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name=kcu.constraint_name
     AND tc.table_schema=kcu.table_schema
   WHERE tc.table_schema=$1
     AND tc.table_name=$2
     AND tc.constraint_type='UNIQUE'
   GROUP BY tc.constraint_name`;
  const r = await db.query(q, [schema, table]);
  // brand_norm, code_norm (필수) + family_slug(있으면 포함) 우선
  let best = null;
  for (const row of r.rows) {
    const cols = row.cols;
    const hasBrand = cols.includes('brand_norm');
    const hasCode  = cols.includes('code_norm');
    if (hasBrand && hasCode) {
      best = cols;
      break;
    }
  }
  return best || ['brand_norm','code_norm'];
}

// pdfimages로 1~2페이지에서 가장 큰 이미지 추출 → GCS 업로드
async function extractCoverToGcs(gcsPdfUri, { family, brand, code }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmpDir  = path.join('/tmp', 'pdf-'+Date.now());
    const pdfPath = path.join(tmpDir, 'doc.pdf');
    await fs.mkdir(tmpDir, { recursive: true });

    // PDF 다운로드
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(pdfPath, buf);

    // pdfimages 설치가 없으면 throw → catch에서 null 반환
    await execFileP('pdfimages', ['-f','1','-l','2','-png', pdfPath, path.join(tmpDir,'img')]); // :contentReference[oaicite:3]{index=3}

    // 가장 큰 PNG 선택
    const files = (await fs.readdir(tmpDir)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!files.length) return null;
    let pick = null, size = -1;
    for (const f of files) {
      const stat = await fs.stat(path.join(tmpDir, f));
      if (stat.size > size) { pick = f; size = stat.size; }
    }
    if (!pick) return null;

    // GCS 업로드
    const dest = canonicalCoverPath(
      process.env.ASSET_BUCKET || process.env.GCS_BUCKET,
      family, brand, code
    );
    const { bucket: dbkt, name: dname } = parseGcsUri(dest);
    await storage.bucket(dbkt).upload(path.join(tmpDir, pick), { destination: dname, resumable: false }); // :contentReference[oaicite:4]{index=4}
    return dest;
  } catch {
    return null;
  }
}

// 동적 UPSERT
async function upsertDynamic(qualifiedTable, payload) {
  const colsSet = await getTableColumns(qualifiedTable);
  const cols = Object.keys(payload).filter(c => colsSet.has(c));
  if (!cols.length) return;

  const conflictCols = (await getConflictColumns(qualifiedTable))
    .filter(c => colsSet.has(c));  // 존재하는 컬럼만

  const names = cols.map(c => `"${c}"`).join(', ');
  const params = cols.map((_, i) => `$${i+1}`).join(', ');
  const updates = cols
    .filter(c => !conflictCols.includes(c))
    .map(c => `"${c}"=EXCLUDED."${c}"`)
    .join(', ');

  const sql = `
    INSERT INTO ${qualifiedTable} (${names})
    VALUES (${params})
    ON CONFLICT (${conflictCols.map(c => `"${c}"`).join(', ')})
    DO UPDATE SET ${updates}, updated_at=now()`;

  await db.query(sql, cols.map(c => payload[c]));
}

// ---------- main ----------
async function runAutoIngest(opts) {
  const { extractDataset } = require('../utils/extract');

  const gcsUri   = opts.gcsUri;
  const filename = opts.filename || gcsUri?.split('/').pop() || 'doc.pdf';

  // 1) 텍스트/코드/브랜드 추출(기존 함수 재사용)
  const ds = await extractDataset({
    gcsUri,
    filename,
    brandHint: opts.brand,
    codeHint: opts.code,
    seriesHint: opts.series
  });

  const brand    = ds.brand || opts.brand || 'unknown';
  const code     = (Array.isArray(ds.rows) && ds.rows[0]?.code) || opts.code || 'TMP_' + Math.random().toString(16).slice(2).toUpperCase();
  const rawText  = ds.text || '';
  const family   = await decideFamily({ explicit: opts.family_slug, text: rawText }) || 'relay_power'; // 기존 기본값 유지

  // 2) 스펙 테이블 보장 + 테이블명 조회
  await db.query('SELECT public.ensure_specs_table($1)', [family]);  // DB 내 ensure 함수 사용
  const reg = await db.query('SELECT specs_table FROM public.component_registry WHERE family_slug=$1 LIMIT 1', [family]);
  const table = `public.${reg.rows[0]?.specs_table || (family + '_specs')}`;

  // 3) 대표 이미지 추출(옵션) + 공통 필드 구성
  const coverUri = await extractCoverToGcs(gcsUri, { family, brand, code }); // 실패 시 null
  const payloadBase = {
    family_slug: family,
    brand,
    code,
    brand_norm: lc(brand),
    code_norm:  lc(code),
    datasheet_uri: gcsUri,      // 테이블에 따라 없을 수 있음 → upsertDynamic에서 필터됨
    datasheet_url: gcsUri,      // 구스키마 호환
    image_uri: coverUri,        // 없으면 upsertDynamic에서 제외됨
    cover: coverUri,            // 구스키마 호환
    width_mm:  null,
    height_mm: null,
    length_mm: null,
    series: ds.series || null,
    display_name: (Array.isArray(ds.rows) && ds.rows[0]?.displayname) || null,
    updated_at: now()
  };

  // 4) 1건만 대표로 넣되, 나중에 rows 루프 확장 가능
  await upsertDynamic(table, payloadBase);

  // 5) 로그(선택) — 기존 테이블/컬럼 있으면 기록
  try {
    await db.query(`
      CREATE TABLE IF NOT EXISTS public.doc_ingest_log(
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
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
      )`);
    await db.query(
      `INSERT INTO public.doc_ingest_log(gcs_uri, filename, brand, family_slug, series_hint, page_count, rows, ms, note)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [gcsUri, filename, brand, family, ds.series || null, null, 1, 0, null]
    );
  } catch {}

  return { ok: true, family_slug: family, table, brand, code };
}

module.exports = { runAutoIngest };
