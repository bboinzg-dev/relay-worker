const db = require('../utils/db');
const { ensureSpecsTable, upsertByBrandCode } = require('../utils/schema');
const { getSignedUrl, canonicalDatasheetPath, canonicalCoverPath, moveObject } = require('../utils/gcs');
const { identifyFamilyBrandCode, extractByBlueprintGemini } = require('../utils/vertex');

async function fetchBlueprint(family_slug) {
  const r = await db.query(`
    SELECT r.specs_table, b.fields_json, b.prompt_template
    FROM public.component_registry r
    JOIN public.component_spec_blueprint b ON b.family_slug = r.family_slug
    WHERE r.family_slug = $1 LIMIT 1`, [family_slug]);
  if (!r.rows.length) throw new Error(`Blueprint not found for family=${family_slug}`);
  return r.rows[0];
}

async function getFamilies() {
  const r = await db.query(`SELECT family_slug FROM public.component_registry ORDER BY family_slug`);
  return r.rows.map(x => x.family_slug);
}

/**
 * Auto ingest pipeline:
 * - Detect {family,brand,code,...} if missing
 * - Fetch blueprint(fields/prompt)
 * - LLM extract (Gemini) into values
 * - ensureSpecsTable + upsert
 * - Move PDF to canonical datasheets path; set datasheet_url / cover placeholder
 */
async function runAutoIngest({ gcsUri, family_slug, brand, code, series=null, display_name=null }) {
  if (!gcsUri) throw new Error('gcsUri required');

  // 1) detection if needed
  if (!family_slug || !brand || !code) {
    const families = await getFamilies();
    const det = await identifyFamilyBrandCode(gcsUri, families);
    family_slug = family_slug || det.family_slug;
    brand = brand || det.brand;
    code = code || det.code;
    series = series || det.series || null;
    display_name = display_name || det.display_name || null;
  }
  if (!family_slug || !brand || !code) throw new Error('Unable to determine family/brand/code');

   const { identifyFamilyBrandCode, extractByBlueprintGemini } = require('../utils/vertex');
+const { normalizeFamilySlug } = require('../utils/family');
@@
   // 1) detection if needed
   if (!family_slug || !brand || !code) {
     const families = await getFamilies();
     const det = await identifyFamilyBrandCode(gcsUri, families);
-    family_slug = family_slug || det.family_slug;
+    family_slug = normalizeFamilySlug(family_slug || det.family_slug);
     brand = brand || det.brand;
     code = code || det.code;


  // 2) blueprint
  const bp = await fetchBlueprint(family_slug);
  const specs_table = bp.specs_table;

  // 3) extraction
  const ext = await extractByBlueprintGemini(gcsUri, bp.fields_json, bp.prompt_template);
  const fields = bp.fields_json || {};
  const values = ext.values || {};

  // 4) ensure table + upsert
  await ensureSpecsTable(specs_table, fields);
  // canonical paths
  const bucket = (process.env.GCS_BUCKET || '').replace(/^gs:\/\//, '').split('/')[0];
  const datasheet_url = canonicalDatasheetPath(bucket, family_slug, brand, code);
  const cover = canonicalCoverPath(bucket, family_slug, brand, code); // TODO: generate cover image
  const row = await upsertByBrandCode(specs_table, {
    brand, code, series, display_name, family_slug, datasheet_url, cover,
    source_gcs_uri: gcsUri, raw_json: ext.raw_json, ...values
  });

  // 5) move file
  try {
    await moveObject(gcsUri, datasheet_url);
  } catch (e) {
    // non-fatal
    console.warn('moveObject failed:', e.message || e);
  }

  return { specs_table, row, detected: { family_slug, brand, code, series, display_name }, fields, values };
}

module.exports = { runAutoIngest };
