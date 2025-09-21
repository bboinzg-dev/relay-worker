// src/utils/schema.js
const db = require('./db');

/**
 * Base columns for specs tables.
 * - brand_norm/code_norm: GENERATED (for new tables we create here)
 * - created_at/updated_at: timestamps (updated_at is managed by trigger in DB)
 */
const BASE_COLUMNS = [
  `id uuid PRIMARY KEY DEFAULT uuid_generate_v4()`,
  `family_slug text`,
  `brand text NOT NULL`,
  `brand_norm text GENERATED ALWAYS AS (lower(brand)) STORED`,
  `code text NOT NULL`,
  `code_norm text GENERATED ALWAYS AS (lower(code)) STORED`,
  `series text`,
  `display_name text`,
  `datasheet_uri text`,
  `image_uri text`,
  `cover text`,
  `raw_json jsonb`,
  `source_gcs_uri text`,
  `created_at timestamptz DEFAULT now()`,
  `updated_at timestamptz DEFAULT now()`
];

function safeIdent(name) {
  return String(name || '').replace(/[^a-zA-Z0-9_]/g, '');
}

/**
 * Ensure the specs table exists with base columns + extra columns (map of name->type).
 */
async function ensureSpecsTable(tableName, extraCols = {}) {
  const safe = safeIdent(tableName);

  // Ensure uuid extension (for uuid_generate_v4)
  await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

  // Create table & unique index if missing
  await db.query(`
    DO $do$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = '${safe}'
      ) THEN
        EXECUTE 'CREATE TABLE public.${safe} ( ${BASE_COLUMNS.join(', ')} )';
        EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS ix_${safe}_brand_code ON public.${safe}(brand_norm, code_norm)';
      END IF;
    END $do$;
  `);

  // Add missing extra columns with simple type mapping
  const mapType = (t) => {
    switch (String(t || '').toLowerCase()) {
      case 'numeric': return 'numeric';
      case 'int':
      case 'integer': return 'integer';
      case 'boolean':
      case 'bool': return 'boolean';
      case 'timestamptz':
      case 'timestamp': return 'timestamptz';
      case 'json':
      case 'jsonb': return 'jsonb';
      case 'text':
      default: return 'text';
    }
  };

  for (const [col, typ] of Object.entries(extraCols || {})) {
    const colSafe = safeIdent(col);
    await db.query(`ALTER TABLE public.${safe} ADD COLUMN IF NOT EXISTS ${colSafe} ${mapType(typ)};`);
  }
}

/**
 * Upsert by (brand_norm, code_norm)
 * - Avoids writing to GENERATED columns (e.g., brand_norm/code_norm on new tables)
 * - Excludes updated_at/created_at/id/brand_norm/code_norm from UPDATE set
 * - If brand_norm/code_norm are not provided (older tables where they are not generated),
 *   we compute them from brand/code.
 */
async function upsertByBrandCode(tableName, values) {
  const safeTable = safeIdent(tableName);
  const payload = { ...(values || {}) };

  // Provide brand_norm/code_norm if absent (for legacy tables where they are plain columns)
  if (payload.brand && payload.brand_norm == null) {
    payload.brand_norm = String(payload.brand).toLowerCase();
  }
  if (payload.code && payload.code_norm == null) {
    payload.code_norm = String(payload.code).toLowerCase();
  }

  // Look up generated columns so we never try to write into them
  const meta = await db.query(
    `SELECT column_name, is_generated
       FROM information_schema.columns
      WHERE table_schema='public' AND table_name=$1`,
    [safeTable]
  );
  const generatedCols = new Set(
    (meta.rows || [])
      .filter(r => String(r.is_generated || '').toUpperCase() === 'ALWAYS')
      .map(r => r.column_name)
  );

  // Build INSERT columns, skipping any generated columns
  const rawCols = Object.keys(payload).map(safeIdent);
  const insertCols = rawCols.filter((c, i) => rawCols.indexOf(c) === i && !generatedCols.has(c));
  if (!insertCols.length) return null;

  const params = insertCols.map((_, i) => `$${i + 1}`);

  // Columns we never update on conflict
  const NO_UPDATE = new Set(['id', 'created_at', 'updated_at', 'brand_norm', 'code_norm']);
  const updateCols = insertCols.filter(c => !NO_UPDATE.has(c));
  const updates = updateCols.map(c => `${c}=EXCLUDED.${c}`);

  const sql = `
    INSERT INTO public.${safeTable} (${insertCols.join(',')})
    VALUES (${params.join(',')})
    ON CONFLICT (brand_norm, code_norm)
    DO UPDATE SET ${[...updates, 'updated_at=now()'].join(', ')}
    RETURNING *`;

  const res = await db.query(sql, insertCols.map(c => payload[c]));
  return (res.rows && res.rows[0]) || null;
}

module.exports = { ensureSpecsTable, upsertByBrandCode };
