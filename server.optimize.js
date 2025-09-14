const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { optimize, optimizeLine } = require('./src/opt/optimizer');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

// POST /api/optimize/plan
// Body: { items: [{brand,code,required_qty,due_date?}, ...], allow_alternatives?, k_alternatives?, use_bids?, lead_penalty_cents_per_unit_per_day?, alternative_penalty_cents_per_unit? }
app.post('/api/optimize/plan', async (req, res) => {
  try {
    const body = req.body || {};
    if (!Array.isArray(body.items) || !body.items.length) return res.status(400).json({ error: 'items[] required' });
    const out = await optimize(body);
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

// GET /api/optimize/line?brand=&code=&qty=&due_date=YYYY-MM-DD
app.get('/api/optimize/line', async (req, res) => {
  try {
    const brand = (req.query.brand || '').toString();
    const code  = (req.query.code  || '').toString();
    const qty   = Number(req.query.qty || 0);
    const due   = (req.query.due_date || '').toString() || null;
    if (!brand || !code || !qty) return res.status(400).json({ error: 'brand, code, qty required' });
    const opts = {
      allow_alternatives: req.query.allow_alternatives !== 'false',
      k_alternatives: Math.min(Math.max(Number(req.query.k_alternatives || 6), 1), 20),
      use_bids: req.query.use_bids !== 'false',
      lead_penalty_cents_per_unit_per_day: Number(req.query.lead_penalty || 10),
      alternative_penalty_cents_per_unit: Number(req.query.alt_penalty || 0),
    };
    const out = await optimizeLine({ brand, code, required_qty: qty, due_date: due }, opts);
    res.json(out);
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
