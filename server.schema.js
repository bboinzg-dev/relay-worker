const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./src/utils/db');
const { loadBlueprint, currentColumns, normalizeFields, diffColumns, sqlForDiff, ensureSchema } = require('./src/schema/manager');
const { runValidation } = require('./src/quality/validator');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

// List families
app.get('/api/schema/families', async (req, res) => {
  try {
    const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }) }
});

// Get status/diff for a family
app.get('/api/schema/status', async (req, res) => {
  try {
    const family = (req.query.family || '').toString();
    if (!family) return res.status(400).json({ error: 'family required' });
    const bp = await loadBlueprint(family);
    const fields = normalizeFields(bp.fields_json);
    const cols = await currentColumns(bp.specs_table);
    const diff = diffColumns(fields, cols);
    const sql = sqlForDiff(bp.specs_table, diff);
    res.json({ family, specs_table: bp.specs_table, fields, columns: cols, diff, recommended_sql: sql });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Ensure schema for a family
app.post('/api/schema/ensure', async (req, res) => {
  try {
    const family = (req.body?.family || '').toString();
    if (!family) return res.status(400).json({ error: 'family required' });
    const bp = await loadBlueprint(family);
    const out = await ensureSchema(bp.specs_table, bp.fields_json);
    // history
    await db.query(`INSERT INTO public.schema_history (family_slug, specs_table, action, statement) VALUES ($1,$2,$3,$4)`, [family, bp.specs_table, 'ensure', JSON.stringify(out.stmts)]).catch(()=>{});
    res.json({ ok: true, specs_table: bp.specs_table, ...out });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// Validate recent rows for a family
app.post('/api/schema/validate', async (req, res) => {
  try {
    const family = (req.body?.family || '').toString();
    const limit = Math.min(Number(req.body?.limit || 500), 5000);
    if (!family) return res.status(400).json({ error: 'family required' });
    const bp = await loadBlueprint(family);
    const issues = await runValidation(bp.specs_table, { family_slug: family, fields_json: bp.fields_json }, limit);
    // upsert issues
    let inserted = 0;
    for (const it of issues) {
      await db.query(`INSERT INTO public.quality_issues
        (family_slug, specs_table, brand_norm, code_norm, field_name, issue_code, severity, message, observed_value, expected, row_pk, meta)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      `, [it.family_slug, it.specs_table, it.brand_norm, it.code_norm, it.field_name, it.issue_code, it.severity, it.message, String(it.observed_value ?? ''), it.expected ? JSON.stringify(it.expected) : null, it.row_pk, it.meta? JSON.stringify(it.meta): null ]).then(()=>{inserted++}).catch(()=>{});
    }
    res.json({ ok: true, specs_table: bp.specs_table, issues_found: issues.length, inserted });
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

// List quality issues
app.get('/api/quality/issues', async (req, res) => {
  try {
    const family = (req.query.family || '').toString() || null;
    const status = (req.query.status || 'open').toString();
    const limit = Math.min(Number(req.query.limit || 500), 5000);
    const wh = [];
    const args = [];
    if (family) { args.push(family); wh.push(`family_slug=$${args.length}`); }
    if (status === 'open') wh.push('resolved_at IS NULL');
    else if (status === 'resolved') wh.push('resolved_at IS NOT NULL');
    const sql = `SELECT * FROM public.quality_issues ${wh.length? 'WHERE ' + wh.join(' AND ') : ''} ORDER BY created_at DESC LIMIT ${limit}`;
    const r = await db.query(sql, args);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

// Resolve quality issue
app.post('/api/quality/issues/:id/resolve', async (req, res) => {
  try {
    const id = req.params.id;
    const who = (req.headers['x-actor-id'] || '').toString() || null;
    await db.query(`UPDATE public.quality_issues SET resolved_at=now(), resolved_by=$2 WHERE id=$1`, [id, who]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

module.exports = app;
