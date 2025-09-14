const db = require('./db');

const BASE_COLUMNS = [
  `id uuid PRIMARY KEY DEFAULT uuid_generate_v4()`,
  `brand text NOT NULL`,
  `code text NOT NULL`,
  `series text`,
  `display_name text`,
  `family_slug text`,
  `datasheet_url text`,
  `cover text`,
  `raw_json jsonb`,
  `source_gcs_uri text`,
  `embedding vector(768) NULL`, -- optional (requires extension)
  `created_at timestamptz NOT NULL DEFAULT now()`,
  `updated_at timestamptz NOT NULL DEFAULT now()`
];

const BASE_COLSET = new Set(BASE_COLUMNS.map(s => s.split(' ')[0]));

async function ensureExtensions() {
  await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);
  try {
    await db.query(`CREATE EXTENSION IF NOT EXISTS vector;`);
  } catch (e) {
    // vector extension might not be available; ignore if fails
  }
}

/**
 * Ensure a specs table exists and contains at least the base columns.
 * Then extend with field columns (text/numeric/int/bool/timestamp/jsonb).
 * fields is an object: { columnName: 'text'|'numeric'|'int'|'bool'|'timestamp'|'jsonb' }
 */
async function ensureSpecsTable(tableName, fields = {}) {
  await ensureExtensions();
  const safe = tableName.replace(/[^a-zA-Z0-9_]/g, '');
  const baseCols = BASE_COLUMNS.join(',\n      ');
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.${safe} (
      ${baseCols}
    );
  `);

  // add lower(brand), lower(code) unique
  await db.query(`
    DO $$ BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname = 'ux_${safe}_brand_code_norm'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX ux_${safe}_brand_code_norm ON public.${safe} (lower(brand), lower(code))';
      END IF;
    END $$;
  `);

  // Add requested fields as columns
  for (const [col, typ] of Object.entries(fields)) {
    const colSafe = col.replace(/[^a-zA-Z0-9_]/g, '');
    const mapType = (t) => {
      switch ((t || '').toLowerCase()) {
        case 'numeric': return 'numeric';
        case 'int':
        case 'integer': return 'integer';
        case 'bool':
        case 'boolean': return 'boolean';
        case 'timestamp':
        case 'timestamptz': return 'timestamptz';
        case 'json':
        case 'jsonb': return 'jsonb';
        case 'text':
        default: return 'text';
      }
    };
    await db.query(`ALTER TABLE public.${safe} ADD COLUMN IF NOT EXISTS ${colSafe} ${mapType(typ)};`);
  }
}

/**
 * Perform an upsert by (lower(brand), lower(code)) uniqueness.
 * values: object of columnName -> value
 */
async function upsertByBrandCode(tableName, values) {
  const safe = tableName.replace(/[^a-zA-Z0-9_]/g, '');
  const cols = Object.keys(values).map(c => c.replace(/[^a-zA-Z0-9_]/g, ''));
  const params = cols.map((_, i) => `$${i+1}`);
  const updates = cols.map(c => `${c}=EXCLUDED.${c}`);
  const sql = `
    INSERT INTO public.${safe} (${cols.join(',')})
    VALUES (${params.join(',')})
    ON CONFLICT ((lower(brand)), (lower(code)))
    DO UPDATE SET ${updates.join(',')}, updated_at = now()
    RETURNING *;
  `;
  const res = await db.query(sql, cols.map(c => values[c]));
  return res.rows[0];
}

module.exports = { ensureSpecsTable, upsertByBrandCode };
