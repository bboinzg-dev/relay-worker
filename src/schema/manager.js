const db = require('../../db');

function mapTypeToPg(t){
  const s = String(t||'text').toLowerCase();
  if (['int','integer'].includes(s)) return 'integer';
  if (['number','numeric','float','double','decimal'].includes(s)) return 'numeric';
  if (['bool','boolean'].includes(s)) return 'boolean';
  if (['json','jsonb'].includes(s)) return 'jsonb';
  if (['vector'].includes(s)) return 'vector(768)'; // default dim
  return 'text';
}

async function loadBlueprint(family){
  const q = await db.query(`
    SELECT r.specs_table, r.family_slug, b.fields_json, b.validators_json, b.prompt_template
    FROM public.component_registry r
    LEFT JOIN public.component_spec_blueprint b USING (family_slug)
    WHERE r.family_slug=$1
    LIMIT 1
  `, [family]);
  if (!q.rows.length) throw new Error('family not found');
  return q.rows[0];
}

async function currentColumns(table){
  const r = await db.query(`
    SELECT column_name, data_type, udt_name
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=$1
    ORDER BY ordinal_position
  `, [table]);
  return r.rows.map(x => ({ name: x.column_name, type: x.udt_name || x.data_type }));
}

function normalizeFields(fields_json){
  const fields = [];
  if (!fields_json) return fields;
  if (Array.isArray(fields_json)) return fields_json;
  for (const [name, def] of Object.entries(fields_json||{})) fields.push({ name, ...(def||{}) });
  return fields;
}

function diffColumns(fields, cols){
  const desired = new Map();
  for (const f of fields) {
    desired.set(f.name, mapTypeToPg(f.type));
  }
  const have = new Map();
  cols.forEach(c => have.set(c.name, c.type));

  const missing = [];
  const typeMismatch = [];
  for (const [name, typ] of desired.entries()) {
    if (!have.has(name)) missing.push({ name, want: typ });
    else {
      const got = have.get(name);
      if (String(got).toLowerCase() !== String(typ).toLowerCase()) typeMismatch.push({ name, have: got, want: typ });
    }
  }
  const extra = [];
  for (const [name, typ] of have.entries()) {
    if (!desired.has(name) && !['id','brand','code','brand_norm','code_norm','series','display_name','family_slug','datasheet_url','cover','source_gcs_uri','embedding','raw_json','tenant_id','owner_id','created_by','updated_by','created_at','updated_at'].includes(name)) {
      extra.push({ name, have: typ });
    }
  }
  return { missing, typeMismatch, extra };
}

function sqlForDiff(table, diff, cols = []){
  const stmts = [];
  for (const m of diff.missing) {
    stmts.push(`ALTER TABLE public.${table} ADD COLUMN IF NOT EXISTS ${m.name} ${m.want};`);
  }
  for (const t of diff.typeMismatch) {
    // safe cast attempt for numeric/text only
    if (String(t.have).startsWith('text') && String(t.want).startsWith('numeric')) {
      stmts.push(`ALTER TABLE public.${table} ALTER COLUMN ${t.name} TYPE numeric USING NULLIF(regexp_replace(${t.name}::text, '[^0-9eE+\-\.]','', 'g'),'')::numeric;`);
    } else if (String(t.want).startsWith('text')) {
      stmts.push(`ALTER TABLE public.${table} ALTER COLUMN ${t.name} TYPE text USING ${t.name}::text;`);
    } else {
      // generic
      stmts.push(`-- REVIEW: ALTER TABLE public.${table} ALTER COLUMN ${t.name} TYPE ${t.want};`);
    }
  }
  const colNames = new Set(cols.map((c) => c.name));

  // legacy index cleanup
  stmts.push(`DROP INDEX IF EXISTS ux_${table}_brand_code_norm;`);
  stmts.push(`DROP INDEX IF EXISTS ix_${table}_trgm_code;`);
  stmts.push(`DROP INDEX IF EXISTS ix_${table}_trgm_brand;`);

  const hasBrandNorm = colNames.has('brand_norm');
  const hasPn = colNames.has('pn');
  const hasPnNorm = colNames.has('pn_norm');
  const hasCodeNorm = colNames.has('code_norm');
  const hasRawJson = colNames.has('raw_json');

  if (hasBrandNorm && hasPn) {
    stmts.push(
      `CREATE UNIQUE INDEX IF NOT EXISTS uq_${table}_brand_pn_und ON public.${table} (brand_norm, pn) NULLS NOT DISTINCT;`
    );
  }

  if (hasBrandNorm) {
    stmts.push(
      `CREATE INDEX IF NOT EXISTS ix_${table}_trgm_brand_norm ON public.${table} USING gin (brand_norm gin_trgm_ops);`
    );
  }
  if (hasPnNorm) {
    stmts.push(
      `CREATE INDEX IF NOT EXISTS ix_${table}_trgm_pn_norm ON public.${table} USING gin (pn_norm gin_trgm_ops);`
    );
  }
  if (hasCodeNorm) {
    stmts.push(
      `CREATE INDEX IF NOT EXISTS ix_${table}_trgm_code_norm ON public.${table} USING gin (code_norm gin_trgm_ops);`
    );
  }
  if (hasRawJson) {
    stmts.push(
      `CREATE INDEX IF NOT EXISTS ix_${table}_raw_json ON public.${table} USING gin (raw_json jsonb_ops);`
    );
  }
  return stmts;
}

async function ensureSchema(table, fields_json){
  const fields = normalizeFields(fields_json);
  const cols = await currentColumns(table);
  const diff = diffColumns(fields, cols);
  const stmts = sqlForDiff(table, diff, cols);
  for (const sql of stmts) {
    await db.query(sql);
  }
  return { diff, stmts };
}

module.exports = { loadBlueprint, currentColumns, normalizeFields, diffColumns, sqlForDiff, ensureSchema, mapTypeToPg };
