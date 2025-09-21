// src/utils/schema.js
const db = require('./db');

/**
 * Base columns compatible with your current DB (relay_power_specs etc.)
 * - datasheet_uri / image_uri / cover columns
 * - brand_norm/code_norm generated columns
 * - created_at/updated_at timestamps
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
  `updated_at timestamptz DEFAULT now()`,
];

/**
 * Ensure the specs table exists with base columns + extra columns (map of name->type).
 */
async function ensureSpecsTable(tableName, extraCols = {}) {
  const safe = tableName.replace(/[^a-zA-Z0-9_]/g, '');

  // ensure required extensions for uuid_generate_v4()
  await db.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";');

  await db.query(`
    DO $do$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name='${safe}'
      ) THEN
        EXECUTE 'CREATE TABLE public.${safe} ( ${BASE_COLUMNS.join(', ')} )';
        EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS ix_${safe}_brand_code ON public.${safe}(brand_norm, code_norm)';
      END IF;
    END $do$;`);

  // Add missing columns (type mapping)
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
    const colSafe = String(col).replace(/[^a-zA-Z0-9_]/g, '');
    await db.query(`ALTER TABLE public.${safe} ADD COLUMN IF NOT EXISTS ${colSafe} ${mapType(typ)};`);
  }
}

/**
 * Upsert by (brand_norm, code_norm)
 */
async function upsertByBrandCode(tableName, values) {
  const safe = tableName.replace(/[^a-zA-Z0-9_]/g, '');
  const cols = Object.keys(values).map(c => c.replace(/[^a-zA-Z0-9_]/g, ''));
  if (!cols.length) return null;
  const params = cols.map((_, i) => `$${i + 1}`);
  const updates = cols.map(c => `${c}=EXCLUDED.${c}`);
  const sql = `
    INSERT INTO public.${safe} (${cols.join(',')})
    VALUES (${params.join(',')})
    ON CONFLICT (brand_norm, code_norm)
    DO UPDATE SET ${updates.join(',')}, updated_at = now()
    RETURNING *`;
  const res = await db.query(sql, cols.map(c => values[c]));
  return res.rows[0] || null;
}

module.exports = { ensureSpecsTable, upsertByBrandCode };
