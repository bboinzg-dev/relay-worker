const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { parseActor, hasRole } = require('./src/utils/auth');
const { runScan, getRules, getRegistry, summarizeTable } = require('./src/quality/scanner');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

function ensureAdmin(req, res, next){
  const actor = parseActor(req);
  if (!hasRole(actor, 'admin')) return res.status(403).json({ error: 'admin required' });
  res.locals.__actor = actor; next();
}

// Run a full scan (per-family or all)
app.post('/api/quality/run', ensureAdmin, async (req, res) => {
  try {
    const family = req.body?.family || null;
    const out = await runScan({ family });
    res.json({ ok: true, runs: out });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

// Quality summary per family
app.get('/api/quality/summary', ensureAdmin, async (req, res) => {
  try {
    const family = (req.query.family || '').toString() || null;
    const reg = await getRegistry(family);
    const out = [];
    for (const r of reg) {
      const bp = await db.query(`SELECT required_fields FROM public.component_spec_blueprint WHERE family_slug=$1`, [r.family_slug]);
      const required = (bp.rows[0]?.required_fields)||[];
      const s = await summarizeTable(r.specs_table, required);
      const counts = (await db.query(`
        SELECT type, severity, COUNT(*)::int AS c
        FROM public.quality_issues
        WHERE family_slug=$1 AND status='open'
        GROUP BY 1,2
      `, [r.family_slug])).rows;
      out.push({ family: r.family_slug, table: r.specs_table, total: s.total, completeness: s.completeness, open_counts: counts });
    }
    res.json({ items: out });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

// Issues list with filters
app.get('/api/quality/issues', ensureAdmin, async (req, res) => {
  try {
    const family = (req.query.family || '').toString();
    const status = (req.query.status || 'open').toString();
    const type = (req.query.type || '').toString();
    const severity = (req.query.severity || '').toString();
    const wh = [], args = [];
    if (family) { args.push(family); wh.push(`family_slug=$${args.length}`); }
    if (status) { args.push(status); wh.push(`status=$${args.length}`); }
    if (type) { args.push(type); wh.push(`type=$${args.length}`); }
    if (severity) { args.push(severity); wh.push(`severity=$${args.length}`); }
    const sql = `SELECT * FROM public.quality_issues ${wh.length?'WHERE '+wh.join(' AND '):''} ORDER BY created_at DESC LIMIT 1000`;
    const r = await db.query(sql, args);
    res.json({ items: r.rows });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

// Accept/resolve issue (wontfix or accept)
app.post('/api/quality/accept/:id', ensureAdmin, async (req, res) => {
  try {
    const id = req.params.id;
    const r = await db.query(`UPDATE public.quality_issues SET status='accepted', accepted_by=$2, resolved_at=now() WHERE id=$1 RETURNING *`, [id, res.locals.__actor.id||null]);
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

// Apply a suggested fix (if suggestion_json present)
app.post('/api/quality/apply-suggested/:id', ensureAdmin, async (req, res) => {
  const client = await db.pool.connect();
  try {
    const id = req.params.id;
    const iq = await client.query(`SELECT * FROM public.quality_issues WHERE id=$1`, [id]);
    if (!iq.rows.length) return res.status(404).json({ error: 'issue not found' });
    const issue = iq.rows[0];
    const sug = issue.suggestion_json || {};
    if (!sug.fix) return res.status(400).json({ error: 'no fix suggestion' });
    await client.query('BEGIN');
    if (sug.fix === 'fill_norms') {
      await client.query(`UPDATE public.${issue.table_name} SET brand_norm=lower(brand) WHERE brand_norm IS NULL`);
      await client.query(`UPDATE public.${issue.table_name} SET code_norm=lower(code) WHERE code_norm IS NULL`);
    } else if (sug.fix === 'fill_display_name') {
      await client.query(`UPDATE public.${issue.table_name} SET display_name=COALESCE(display_name, concat(brand,' ',code)) WHERE display_name IS NULL OR display_name=''`);
    } else {
      // no-op for dedupe / complex fixes (manual)
    }
    const updated = await client.query(`UPDATE public.quality_issues SET status='fixed', fixed_by=$2, resolved_at=now() WHERE id=$1 RETURNING *`, [id, (req.headers['x-actor-id']||'')]);
    await client.query('COMMIT');
    res.json({ ok: true, item: updated.rows[0] });
  } catch (e) { try { await db.query('ROLLBACK'); } catch {} console.error(e); res.status(400).json({ error: String(e.message || e) }); }
  finally { client.release(); }
});

// Bulk fixes
app.post('/api/quality/fix', ensureAdmin, async (req, res) => {
  const client = await db.pool.connect();
  try {
    const { family, op } = req.body || {};
    if (!family) return res.status(400).json({ error: 'family required' });
    const reg = await db.query(`SELECT specs_table FROM public.component_registry WHERE family_slug=$1`, [family]);
    if (!reg.rows.length) return res.status(404).json({ error: 'family not found' });
    const table = reg.rows[0].specs_table;
    await client.query('BEGIN');
    let result = {};
    if (op === 'fill_norms') {
      const r1 = await client.query(`UPDATE public.${table} SET brand_norm=lower(brand) WHERE brand_norm IS NULL`);
      const r2 = await client.query(`UPDATE public.${table} SET code_norm=lower(code) WHERE code_norm IS NULL`);
      result = { updated: (r1.rowCount||0)+(r2.rowCount||0) };
    } else if (op === 'fill_display_name') {
      const r = await client.query(`UPDATE public.${table} SET display_name=COALESCE(display_name, concat(brand,' ',code)) WHERE display_name IS NULL OR display_name=''`);
      result = { updated: r.rowCount||0 };
    } else {
      return res.status(400).json({ error: 'unsupported op' });
    }
    await client.query('COMMIT');
    res.json({ ok: true, result });
  } catch (e) { try { await client.query('ROLLBACK'); } catch {}; console.error(e); res.status(400).json({ error: String(e.message || e) }); }
  finally { client.release(); }
});

// Rules: get/update
app.get('/api/quality/rules', ensureAdmin, async (req, res) => {
  try {
    const family = (req.query.family || '').toString();
    if (!family) return res.status(400).json({ error: 'family required' });
    const r = await db.query(`SELECT rules_json FROM public.quality_rules WHERE family_slug=$1`, [family]);
    res.json({ rules: (r.rows[0]?.rules_json)||{} });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

app.post('/api/quality/rules', ensureAdmin, async (req, res) => {
  try {
    const { family, rules } = req.body || {};
    if (!family) return res.status(400).json({ error: 'family required' });
    await db.query(`
      INSERT INTO public.quality_rules (family_slug, rules_json, updated_at)
      VALUES ($1,$2,now())
      ON CONFLICT (family_slug) DO UPDATE SET rules_json=EXCLUDED.rules_json, updated_at=now()
    `, [family, rules || {}]);
    res.json({ ok: true });
  } catch (e) { console.error(e); res.status(400).json({ error: String(e.message || e) }); }
});

module.exports = app;
