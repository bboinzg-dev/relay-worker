const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { optimize, optimizeLine } = (()=>{ try { return require('./src/opt/optimizer'); } catch { return { optimize: async(b)=>b, optimizeLine: async(b)=>b }; } })();
const { ilpOptimize, ilpOptimizeLine } = (()=>{ try { return require('./src/opt/ilp'); } catch { return { ilpOptimize: async(b)=>b, ilpOptimizeLine: async(b)=>b }; } })();

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

app.post('/api/optimize/plan', async (req, res) => {
  try {
    const body = req.body || {};
    if (!Array.isArray(body.items) || !body.items.length) return res.status(400).json({ error: 'items[] required' });
    const solver = (body.solver || 'greedy').toString();
    const out = solver === 'ilp' ? await ilpOptimize(body) : await optimize(body);
    res.json({ solver, ...out });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

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
    const solver = (req.query.solver || 'greedy').toString();
    const out = solver === 'ilp' ? await ilpOptimizeLine({ brand, code, required_qty: qty, due_date: due }, opts)
                                 : await optimizeLine({ brand, code, required_qty: qty, due_date: due }, opts);
    res.json({ solver, ...out });
  } catch (e) {
    console.error(e);
    res.status(400).json({ error: String(e.message || e) });
  }
});

module.exports = app;
