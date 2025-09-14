const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { analyzeBom, persistPlan } = require('./src/pipeline/bom');
const { parseActor } = (()=>{ try { return require('./src/utils/auth'); } catch { return { parseActor: ()=>({}) }; } })();

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));

// POST /api/bom/analyze { upload_id? | rows: [{brand,code,qty,need_by?}] }
app.post('/api/bom/analyze', async (req, res) => {
  try {
    const { upload_id=null, rows=null } = req.body || {};
    const result = await analyzeBom({ upload_id, rows });
    res.json(result);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// POST /api/bom/plan { plan_items: [... analyze output items ...], execute?: boolean }
app.post('/api/bom/plan', async (req, res) => {
  try {
    const actor = parseActor(req);
    const { plan_items=[], execute=true } = req.body || {};
    if (!Array.isArray(plan_items) || !plan_items.length) return res.status(400).json({ error: 'plan_items[] required' });
    if (!execute) return res.json({ ok: true, dry_run: true });
    const out = await persistPlan({ plan_items, actor, tenant_id: actor.tenantId || null });
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
