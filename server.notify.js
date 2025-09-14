const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./src/utils/db');
const { parseActor } = require('./src/utils/auth');
const { notify, findFamilyForBrandCode, recordEvent } = require('./src/utils/notify');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '25mb' }));

// manual dispatch
// POST /notify/dispatch { type, payload, family_slug?, brand?, code?, targets? }
app.post('/notify/dispatch', async (req, res) => {
  try {
    const actor = parseActor(req);
    const { type, payload={}, family_slug=null, brand=null, code=null } = req.body || {};
    if (!type) return res.status(400).json({ error: 'type required' });
    const out = await notify(type, {
      tenant_id: actor.tenantId || null,
      actor_id: actor.id || null,
      family_slug, brand, code,
      data: payload
    });
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// test event log
app.get('/notify/test', async (req, res) => {
  const actor = parseActor(req);
  const evt = await recordEvent({ type: 'test', tenant_id: actor.tenantId || null, actor_id: actor.id || null, payload: { hello: 'world' } });
  res.json(evt);
});

// helper to resolve family for brand/code
app.get('/notify/family', async (req, res) => {
  const brand = (req.query.brand || '').toString();
  const code = (req.query.code || '').toString();
  if (!brand || !code) return res.status(400).json({ error: 'brand & code required' });
  const family = await findFamilyForBrandCode(brand, code);
  res.json({ family_slug: family });
});

module.exports = app;
