const db = require('../utils/db');

async function getRegistry(family){
  if (family) {
    const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry WHERE family_slug=$1`, [family]);
    return r.rows;
  }
  const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`);
  return r.rows;
}

async function getBlueprintValidators(family){
  try {
    const r = await db.query(`SELECT validators_json, required_fields FROM public.component_spec_blueprint WHERE family_slug=$1`, [family]);
    const row = r.rows[0] || {};
    return { validators: row.validators_json || {}, required: row.required_fields || [] };
  } catch { return { validators:{}, required:[] }; }
}

async function getRules(family){
  const r = await db.query(`SELECT rules_json FROM public.quality_rules WHERE family_slug=$1`, [family]);
  const row = r.rows[0] || {};
  return row.rules_json || {};
}

function norm(s){ return (s==null? null : String(s).trim().toLowerCase()); }

async function summarizeTable(table, requiredFields=[]){
  // completeness per field
  const fields = Array.from(new Set(['brand','code','display_name','datasheet_url','cover', ...requiredFields]));
  const sel = fields.map(f => `SUM(CASE WHEN ${f} IS NULL OR ${f}::text='' THEN 1 ELSE 0 END) AS missing_${f}`).join(', ');
  const r = await db.query(`SELECT COUNT(*)::int AS total, ${sel} FROM public.${table}`);
  const row = r.rows[0] || { total: 0 };
  const completeness = {};
  for (const f of fields) {
    const miss = Number(row['missing_'+f] || 0);
    completeness[f] = { missing: miss, present: Math.max(0, row.total - miss) };
  }
  return { total: Number(row.total||0), completeness };
}

async function findDuplicates(table){
  const r = await db.query(`
    SELECT brand_norm, code_norm, COUNT(*)::int AS c,
           array_agg(jsonb_build_object('brand', brand, 'code', code)) AS rows
    FROM public.${table}
    WHERE brand_norm IS NOT NULL AND code_norm IS NOT NULL
    GROUP BY brand_norm, code_norm HAVING COUNT(*) > 1
    ORDER BY c DESC LIMIT 500
  `);
  return r.rows;
}

async function numericColumns(table){
  const r = await db.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=$1 AND data_type IN ('integer','numeric','real','double precision')
  `, [table]);
  return r.rows.map(x => x.column_name);
}

async function outliers(table, col){
  // IQR method
  const r = await db.query(`
    SELECT
      percentile_cont(0.25) WITHIN GROUP (ORDER BY ${col}) AS q1,
      percentile_cont(0.75) WITHIN GROUP (ORDER BY ${col}) AS q3
    FROM public.${table}
    WHERE ${col} IS NOT NULL
  `);
  const q1 = Number(r.rows[0]?.q1 ?? 0), q3 = Number(r.rows[0]?.q3 ?? 0);
  const iqr = q3 - q1;
  if (!isFinite(iqr) || iqr<=0) return [];
  const lo = q1 - 1.5 * iqr, hi = q3 + 1.5 * iqr;
  const rr = await db.query(`
    SELECT brand, code, brand_norm, code_norm, ${col} AS value
    FROM public.${table}
    WHERE ${col} IS NOT NULL AND (${col} < $1 OR ${col} > $2)
    ORDER BY ${col} ASC LIMIT 500
  `, [lo, hi]);
  return rr.rows.map(x => ({ ...x, bounds: { lo, hi } }));
}

async function runScanOne({ family, table, runId, rules }){
  const ret = { issues: [], counts: { error:0, warn:0, info:0 } };

  const validatorsBp = await getBlueprintValidators(family);
  const requiredFields = Array.from(new Set([...(validatorsBp.required||[]), ...((rules.required_fields)||[])]));

  // Completeness summary (create 'link_missing' issues for datasheet/cover if missing ratio high)
  const sum = await summarizeTable(table, requiredFields);
  for (const f of requiredFields) {
    const miss = sum.completeness[f]?.missing || 0;
    if (miss > 0) {
      ret.issues.push({
        family_slug: family, table_name: table, row_ref: null, field: f, type: 'missing',
        severity: f==='brand'||f==='code' ? 'error' : 'warn',
        message: `Field '${f}' is missing for ${miss}/${sum.total} rows`,
        suggestion_json: null, run_id: runId
      });
      ret.counts[f==='brand'||f==='code' ? 'error':'warn'] += 1;
    }
  }
  for (const f of ['datasheet_url','cover']) {
    const miss = sum.completeness[f]?.missing || 0;
    if (miss > 0) {
      ret.issues.push({
        family_slug: family, table_name: table, row_ref: null, field: f, type: 'link_missing',
        severity: 'info',
        message: `${f} missing for ${miss}/${sum.total} rows`,
        suggestion_json: null, run_id: runId
      });
      ret.counts.info += 1;
    }
  }

  // Norm fields fillability
  const rn = await db.query(`SELECT COUNT(*)::int AS c FROM public.${table} WHERE brand_norm IS NULL OR code_norm IS NULL`);
  if (Number(rn.rows[0]?.c || 0) > 0) {
    ret.issues.push({
      family_slug: family, table_name: table, row_ref: null, field: 'brand_norm/code_norm', type: 'normalization',
      severity: 'warn',
      message: `brand_norm or code_norm is NULL for ${rn.rows[0].c} rows`,
      suggestion_json: { fix: 'fill_norms' }, run_id: runId
    });
    ret.counts.warn += 1;
  }

  // Duplicates
  const dups = await findDuplicates(table);
  for (const d of dups) {
    ret.issues.push({
      family_slug: family, table_name: table, row_ref: `${d.brand_norm}/${d.code_norm}`, field: 'brand_norm,code_norm', type: 'duplicate',
      severity: 'error',
      message: `Duplicate rows for ${d.brand_norm}/${d.code_norm}: count=${d.c}`,
      suggestion_json: { fix: 'dedupe_keep', key: { brand_norm: d.brand_norm, code_norm: d.code_norm }, policy: 'keep_newest' },
      run_id: runId
    });
    ret.counts.error += 1;
  }

  // Numeric outliers (if configured or default allow)
  const numericCols = await numericColumns(table);
  const outlierCols = (rules.outlier_cols && rules.outlier_cols.length) ? rules.outlier_cols.filter(c=>numericCols.includes(c)) : numericCols.slice(0, 4);
  for (const c of outlierCols) {
    const outs = await outliers(table, c);
    if (outs.length) {
      ret.issues.push({
        family_slug: family, table_name: table, row_ref: null, field: c, type: 'outlier',
        severity: 'info',
        message: `Outliers detected for ${c}: ${outs.length} rows (IQR)`,
        suggestion_json: { sample: outs.slice(0, 5) }, run_id: runId
      });
      ret.counts.info += 1;
    }
  }

  return ret;
}

async function saveIssues(runId, issues){
  if (!issues.length) return 0;
  const cols = ['family_slug','table_name','row_ref','field','type','severity','message','suggestion_json','run_id'];
  const values = issues.map((it,i)=>`($${i*9+1},$${i*9+2},$${i*9+3},$${i*9+4},$${i*9+5},$${i*9+6},$${i*9+7},$${i*9+8},$${i*9+9})`).join(',');
  const args = [];
  for (const it of issues) {
    args.push(it.family_slug, it.table_name, it.row_ref, it.field, it.type, it.severity, it.message, it.suggestion_json, runId);
  }
  await db.query(`INSERT INTO public.quality_issues (${cols.join(',')}) VALUES ${values}`, args);
  return issues.length;
}

async function runScan({ family, limitPerFamily=100000 }){
  const reg = await getRegistry(family);
  const runs = [];
  for (const r of reg) {
    const run = (await db.query(`INSERT INTO public.quality_runs (family_slug) VALUES ($1) RETURNING *`, [r.family_slug])).rows[0];
    try {
      // load rules
      const rules = await getRules(r.family_slug);
      const out = await runScanOne({ family: r.family_slug, table: r.specs_table, runId: run.id, rules });
      await saveIssues(run.id, out.issues);
      await db.query(`UPDATE public.quality_runs SET status='succeeded', finished_at=now(), counts=$2 WHERE id=$1`, [run.id, out.counts]);
      runs.push({ run: run.id, family: r.family_slug, counts: out.counts, issues: out.issues.length });
    } catch (e) {
      await db.query(`UPDATE public.quality_runs SET status='failed', finished_at=now(), counts=$2 WHERE id=$1`, [run.id, { error: String(e.message || e) }]);
      runs.push({ run: run.id, family: r.family_slug, error: String(e.message || e) });
    }
  }
  return runs;
}

module.exports = { runScan, getRules, getRegistry, runScanOne, summarizeTable };
