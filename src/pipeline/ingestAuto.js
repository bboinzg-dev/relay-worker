// src/pipeline/ingestAuto.js
'use strict';

/**
 * Generic ingestion pipeline (keeps legacy behaviour).
 * - If family_slug is provided, use it.
 * - Otherwise, try a light-weight heuristic to guess family from file name/text.
 * - Looks up specs_table in component_registry and ensures the table exists.
 * - Tries to extract a cover image using `pdfimages` (if present); failure is ignored.
 * - Upserts only columns that actually exist on the destination table.
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
  if (/\b(resistor|r-clamp|ohm)\b/.test(s)) return 'resistor';
  if (/\b(capacitor|mlcc|electrolytic|tantalum)\b/.test(s)) return 'capacitor_mlcc';
  if (/\b(inductor|choke)\b/.test(s)) return 'inductor';
  if (/\b(bridge|rectifier|diode)\b/.test(s)) return 'bridge_rectifier';
  if (/\b(relay|coil|omron|finder)\b/.test(s)) return 'relay_power';
  return null;
}

// ---- main ------------------------------------------------------------------

async function runAutoIngest({
  gcsUri,
  family_slug = null,
  brand = null,
  code = null,
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

  // 2) Ensure the table exists (keep base columns compatible with your DB)
  await ensureSpecsTable(table, {
    datasheet_uri: 'text',
    image_uri: 'text',
    width_mm: 'numeric',
    height_mm: 'numeric',
    length_mm: 'numeric',
  });

  // 3) Best-effort cover extraction
  let coverUri = null;
  try {
    coverUri = await extractCoverToGcs(gcsUri, {
      family,
      brand: brand || '',
      code:  code || '',
    });
  } catch {}

  // 4) Build payload (only safe keys; upsert is by (brand_norm, code_norm))
  const payload = {
    family_slug: family,
    brand: brand || 'unknown',
    code:  code  || path.parse(fileName).name,
    series: series || null,
    display_name: display_name || null,
    datasheet_uri: gcsUri,
    image_uri: coverUri || null,
    updated_at: new Date(), // ignored if column doesn't exist
  };

  // 5) Upsert (uses only existing columns)
  const colsSet = await getTableColumns(table.startsWith('public.') ? table : `public.${table}`);
  const safePayload = {};
  for (const [k, v] of Object.entries(payload)) {
    if (colsSet.has(k)) safePayload[k] = v;
  }
  const row = await upsertByBrandCode(table, safePayload);

  return {
    ok: true,
    ms: Date.now() - started,
    family,
    specs_table: table,
    brand: row?.brand || payload.brand,
    code: row?.code || payload.code,
    datasheet_uri: gcsUri,
    cover: coverUri,
    rows: 1,
  };
}

module.exports = { runAutoIngest };
