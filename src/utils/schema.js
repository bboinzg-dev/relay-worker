const db = require('./db');

const BASE_COLUMNS = [
  `id uuid PRIMARY KEY DEFAULT uuid_generate_v4()`,
  `brand text NOT NULL`,
  `code text NOT NULL`,
  `brand_norm text GENERATED ALWAYS AS (lower(brand)) STORED`,
  `code_norm text GENERATED ALWAYS AS (lower(code)) STORED`,
  `series text`,
  `display_name text`,
  `family_slug text`,
  `datasheet_url text`,
  `image_url text`, // 대표 이미지(GCS)
  `cover text`,     // (하위호환)
  `length_mm numeric`,
  `width_mm  numeric`,
  `height_mm numeric`,
  `raw_json jsonb`,
  `source_gcs_uri text`,
  `embedding vector(768) NULL`,
  `created_at timestamptz NOT NULL DEFAULT now()`,
  `updated_at timestamptz NOT NULL DEFAULT now()`
];

async function ensureExtensions() {
  await db.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);
  try { await db.query(`CREATE EXTENSION IF NOT EXISTS vector;`); } catch {}
}

async function ensureSpecsTable(tableName, fields = {}) {
  await ensureExtensions();
  const safe = tableName.replace(/[^a-zA-Z0-9_]/g, '');
  const baseCols = BASE_COLUMNS.join(',\n      ');
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.${safe} (
      ${baseCols}
    );
  `);

  await db.query(`
    DO $$ BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_${safe}_brand_norm_code_norm'
      ) THEN
        ALTER TABLE public.${safe}
        ADD CONSTRAINT uq_${safe}_brand_norm_code_norm UNIQUE (brand_norm, code_norm);
      END IF;
    END $$;
  `);

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

async function upsertByBrandCode(tableName, values) {
  const safe = tableName.replace(/[^a-zA-Z0-9_]/g, '');
  const cols = Object.keys(values).map(c => c.replace(/[^a-zA-Z0-9_]/g, ''));
  const params = cols.map((_, i) => `$${i+1}`);
  const updates = cols.map(c => `${c}=EXCLUDED.${c}`);
  const sql = `
    INSERT INTO public.${safe} (${cols.join(',')})
    VALUES (${params.join(',')})
    ON CONFLICT (brand_norm, code_norm)
    DO UPDATE SET ${updates.join(',')}, updated_at = now()
    RETURNING *;
  `;
  const res = await db.query(sql, cols.map(c => values[c]));
  return res.rows[0];
}

module.exports = { ensureSpecsTable, upsertByBrandCode };
