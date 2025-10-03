const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./db');
const { parseActor, hasRole } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '5mb' }));

function ensureSellerOrAdmin(req, res, next){
  const actor = parseActor(req);
  if (!(hasRole(actor,'seller') || hasRole(actor,'admin'))) return res.status(403).json({ error: 'seller role required' });
  res.locals.__actor = actor; next();
}

app.get('/api/subscriptions', ensureSellerOrAdmin, async (req, res) => {
  try {
    const actor = res.locals.__actor || {};
    const r = await db.query(`SELECT * FROM public.subscriptions WHERE actor_id=$1 ORDER BY created_at DESC`, [actor.id||'']);
    res.json({ items: r.rows });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

app.post('/api/subscriptions', ensureSellerOrAdmin, async (req, res) => {
  try {
    const actor = res.locals.__actor || {};
    const { family_slug, target_email, target_webhook, active=true } = req.body || {};
    if (!family_slug) return res.status(400).json({ error: 'family_slug required' });
    const r = await db.query(`INSERT INTO public.subscriptions(actor_id, tenant_id, family_slug, target_email, target_webhook, active) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [actor.id||'', actor.tenantId||null, family_slug, target_email||null, target_webhook||null, !!active]);
    res.json({ ok: true, item: r.rows[0] });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

// 간단한 테스트 발송
app.post('/api/notify/test', ensureSellerOrAdmin, async (req, res) => {
  try {
    const actor = res.locals.__actor || {};
    await db.query(`INSERT INTO public.notifications(type, payload, target, channel) VALUES ('test', $1, $2, 'email')`, [ { hello:'world', at: new Date().toISOString() }, actor.id || 'me' ]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

module.exports = app;
