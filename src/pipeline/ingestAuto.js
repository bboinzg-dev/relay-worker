/* relay-worker/src/pipeline/ingestAuto.js */
'use strict';

const path = require('node:path');
const fs = require('node:fs/promises');
const os = require('node:os');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);

const db = require('../utils/db');
const { storage, parseGcsUri, canonicalCoverPath } = require('../utils/gcs');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');

// 첫 1~2페이지에서 가장 큰 이미지를 골라 GCS 업로드
// pdfimages가 없거나 실패하면 null 반환 (완전 무시)
async function tryExtractCover(gcsPdfUri, { family, brand, code }) {
  try {
    const { bucket, name } = parseGcsUri(gcsPdfUri);
    const tmpDir = path.join(os.tmpdir(), 'pdf-' + Date.now());
    await fs.mkdir(tmpDir, { recursive: true });
    const localPdf = path.join(tmpDir, 'doc.pdf');
    const [buf] = await storage.bucket(bucket).file(name).download();
    await fs.writeFile(localPdf, buf);

    // pdfimages 호출 (미설치/실패 시 조용히 무시)
    try {
      await execFileP('pdfimages', ['-f','1','-l','2','-png', localPdf, path.join(tmpDir, 'img')]);
    } catch {
      return null;
    }

    const files = (await fs.readdir(tmpDir)).filter(f => /^img-\d+-\d+\.png$/i.test(f));
    if (!files.length) return null;

    let pick = null, size = -1;
    for (const f of files) {
      const st = await fs.stat(path.join(tmpDir, f));
      if (st.size > size) { pick = f; size = st.size; }
    }
    if (!pick) return null;

    const dst = canonicalCoverPath(process.env.ASSET_BUCKET || process.env.GCS_BUCKET, family, brand, code);
    const { bucket: dbkt, name: dname } = parseGcsUri(dst);
    await storage.bucket(dbkt).upload(path.join(tmpDir, pick), { destination: dname, resumable: false });
    return dst;
  } catch {
    return null;
  }
}

// family가 없을 때 최소 탐색(기존 데이터에서 찾아보고 없으면 relay_power)
async function detectFamily({ family_slug, brand, code }) {
  if (family_slug) return family_slug;
  try {
    const r = await db.query(`
      select family_slug from public.component_specs
       where brand_norm = lower($1) and code_norm = lower($2)
       limit 1`, [brand || '', code || '']);
    if (r.rows[0]?.family_slug) return r.rows[0].family_slug;
  } catch {}
  return 'relay_power';
}

async function runAutoIngest({ gcsUri, family_slug, brand, code, series=null, display_name=null }) {
  const started = Date.now();

  // 1) family / table 결정
  const family = await detectFamily({ family_slug, brand, code });
  const reg = await db.query(`select specs_table from public.component_registry where family_slug=$1 limit 1`, [family]);
  const table = reg.rows[0]?.specs_table || 'relay_power_specs';

  await ensureSpecsTable(table, {}); // 테이블 보장

  // 2) 최소 행 upsert (추후 추출 프로세스가 갱신)
  const row = await upsertByBrandCode(table, {
    family_slug: family,
    brand, code, series, display_name,
    datasheet_uri: gcsUri,
  });

  // 3) 대표 이미지 추출(실패/미설치 무시)
  try {
    const coverUri = await tryExtractCover(gcsUri, { family, brand, code });
    if (coverUri) {
      await db.query(`update public.${table}
                        set image_uri = coalesce(image_uri, $1), updated_at = now()
                      where brand_norm = lower($2) and code_norm = lower($3)`,
        [coverUri, brand, code]);
    }
  } catch {}

  return {
    ok: true,
    ms: Date.now() - started,
    family, specs_table: `public.${table}`,
    brand, code,
    datasheet_uri: gcsUri,
    rows: 1,
  };
}

module.exports = { runAutoIngest };
