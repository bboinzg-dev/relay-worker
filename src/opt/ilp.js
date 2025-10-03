const db = require('../../db');
const solver = require('javascript-lp-solver');

function daysUntil(due) {
  if (!due) return null;
  try {
    const d = new Date(due); const now = new Date();
    return Math.ceil((d.getTime() - now.getTime())/86400000);
  } catch { return null; }
}

async function getKnownSpecsTables() {
  const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`);
  return r.rows;
}
async function findExactRow(brand, code) {
  const regs = await getKnownSpecsTables();
  for (const r of regs) {
    const q = await db.query(`SELECT * FROM public/${r.specs_table} WHERE (brand_norm=lower($1) OR lower(brand)=lower($1)) AND (code_norm=lower($2) OR lower(code)=lower($2)) LIMIT 1`, [brand, code]);
    if (q.rows.length) return { table: r.specs_table, family_slug: q.rows[0].family_slug || r.family_slug, row: q.rows[0] };
  }
  return null;
}
async function getAlternativesFor(table, baseRow, k=6) {
  try {
    if (baseRow.embedding) {
      const q = await db.query(`SELECT *, (embedding <=> $1::vector) AS dist FROM public/${table} WHERE NOT (brand_norm=$2 AND code_norm=$3) ORDER BY embedding <=> $1::vector LIMIT $4`,
        [baseRow.embedding, baseRow.brand_norm || baseRow.brand?.toLowerCase(), baseRow.code_norm || baseRow.code?.toLowerCase(), k]);
      return { mode: 'embedding', items: q.rows };
    }
  } catch {}
  const q2 = await db.query(`
    SELECT *,
      (CASE WHEN family_slug IS NOT NULL AND family_slug = $1 THEN 0 ELSE 1 END) * 1.0 +
      COALESCE(ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($2::numeric,0)) / 100.0, 1.0) AS score
    FROM public/${table}
    WHERE NOT (brand_norm=$3 AND code_norm=$4)
    ORDER BY score ASC
    LIMIT $5
  `, [baseRow.family_slug || null, baseRow.coil_voltage_vdc || null, baseRow.brand_norm || baseRow.brand?.toLowerCase(), baseRow.code_norm || baseRow.code?.toLowerCase(), k]);
  return { mode: 'rule-fallback', items: q2.rows };
}

async function gatherListings(brand, code) {
  const r = await db.query(`
    SELECT id, brand, code, brand_norm, code_norm, price_cents, currency, quantity_available, lead_time_days, seller_ref
    FROM public.listings
    WHERE brand_norm=lower($1) AND code_norm=lower($2) AND quantity_available > 0
    ORDER BY price_cents ASC, lead_time_days NULLS FIRST, created_at DESC
  `, [brand, code]);
  return r.rows.map(x => ({
    source: 'listing',
    id: x.id,
    brand: x.brand, code: x.code,
    is_alternative: false,
    unit_price_cents: Number(x.price_cents || 0),
    currency: x.currency || 'USD',
    available_qty: Number(x.quantity_available || 0),
    lead_time_days: x.lead_time_days == null ? null : Number(x.lead_time_days),
    meta: { seller_ref: x.seller_ref }
  }));
}
async function gatherBidsForSku(brand, code) {
  const r = await db.query(`
    SELECT b.*, pr.brand, pr.code
    FROM public.bids b
    JOIN public.purchase_requests pr ON pr.id = b.purchase_request_id
    WHERE pr.brand_norm=lower($1) AND pr.code_norm=lower($2)
    ORDER BY b.price_cents ASC, b.lead_time_days NULLS FIRST, b.created_at DESC
  `, [brand, code]);
  return r.rows.map(x => ({
    source: 'bid', id: x.id,
    brand: x.brand, code: x.code,
    is_alternative: false,
    unit_price_cents: Number(x.price_cents || 0),
    currency: x.currency || 'USD',
    available_qty: Number(x.offer_qty || 0),
    lead_time_days: x.lead_time_days == null ? null : Number(x.lead_time_days),
    meta: { purchase_request_id: x.purchase_request_id }
  }));
}
async function gatherAltBidsFor(brand, code) {
  const r = await db.query(`
    SELECT * FROM public.bids
    WHERE is_alternative = true AND lower(alt_brand)=lower($1) AND lower(alt_code)=lower($2)
    ORDER BY price_cents ASC, lead_time_days NULLS FIRST, created_at DESC
  `, [brand, code]);
  return r.rows.map(x => ({
    source: 'bid', id: x.id,
    brand: x.alt_brand, code: x.alt_code,
    is_alternative: true,
    unit_price_cents: Number(x.price_cents || 0),
    currency: x.currency || 'USD',
    available_qty: Number(x.offer_qty || 0),
    lead_time_days: x.lead_time_days == null ? null : Number(x.lead_time_days),
    meta: { purchase_request_id: x.purchase_request_id }
  }));
}

function coefficientFor(offer, { due_date=null, lead_penalty_cents_per_unit_per_day=10, alternative_penalty_cents_per_unit=0 }) {
  const unit = Number(offer.unit_price_cents || 0);
  let leadPenalty = 0;
  if (due_date && offer.lead_time_days != null) {
    const days = daysUntil(due_date);
    if (days != null) {
      const delay = Math.max(0, offer.lead_time_days - days);
      leadPenalty = delay * Number(lead_penalty_cents_per_unit_per_day || 0);
    }
  }
  const alt = offer.is_alternative ? Number(alternative_penalty_cents_per_unit || 0) : 0;
  return unit + leadPenalty + alt;
}

function buildModel(required_qty, offers, penaltyCfg) {
  const variables = {};
  const constraints = { demand: { min: Number(required_qty || 0) } };
  let intVars = {};
  offers.forEach((o, idx) => {
    const name = `x_${idx}`;
    const coef = coefficientFor(o, penaltyCfg);
    variables[name] = {
      cost: coef,
      demand: 1,
    };
    constraints[name] = { max: Number(o.available_qty || 0) };
    variables[name][name] = 1;
    // Treat as integer? For discrete units we can set integers; keep continuous first for speed.
    // intVars[name] = 1;
  });
  const model = {
    optimize: 'cost',
    opType: 'min',
    constraints,
    variables,
    // integers: intVars,  // enable if strictly integer is needed
  };
  return model;
}

function readSolution(soln, offers) {
  const out = [];
  let remaining = 0;
  for (const [name, v] of Object.entries(soln)) {
    if (!name.startsWith('x_')) continue;
    const idx = Number(name.split('_')[1]);
    const qty = Math.max(0, Math.round(v));
    if (qty <= 0) continue;
    const o = offers[idx];
    out.push({
      source: o.source,
      offer_id: o.id,
      brand: o.brand, code: o.code,
      qty,
      unit_price_cents: o.unit_price_cents,
      effective_unit_cents: o.unit_price_cents, // ILP already included penalties in objective
      lead_time_days: o.lead_time_days,
      is_alternative: !!o.is_alternative,
      currency: o.currency || 'USD',
      meta: o.meta || {},
    });
    remaining += qty;
  }
  return out;
}

async function ilpOptimizeLine({ brand, code, required_qty, due_date=null }, { allow_alternatives=true, k_alternatives=6, use_bids=true, lead_penalty_cents_per_unit_per_day=10, alternative_penalty_cents_per_unit=0 } = {}) {
  // 1) enumerate offers
  const offers = [];
  offers.push(...await gatherListings(brand, code));
  if (use_bids) offers.push(...await gatherBidsForSku(brand, code));
  if (allow_alternatives) {
    const base = await findExactRow(brand, code);
    if (base) {
      const alts = await getAlternativesFor(base.table, base.row, k_alternatives);
      for (const r of alts.items) {
        offers.push(...(await gatherListings(r.brand || r.brand_norm, r.code || r.code_norm)).map(x => ({ ...x, is_alternative: true })));
        if (use_bids) offers.push(...(await gatherAltBidsFor(r.brand || r.brand_norm, r.code || r.code_norm)));
      }
    }
  }
  // 2) build LP model
  const penaltyCfg = { due_date, lead_penalty_cents_per_unit_per_day, alternative_penalty_cents_per_unit };
  const model = buildModel(required_qty, offers, penaltyCfg);
  // 3) solve
  const res = solver.Solve(model);
  if (!res.feasible) {
    return {
      input: { brand, code, required_qty, due_date },
      offers_count: offers.length,
      solver: 'ilp',
      feasible: false,
      plan: { assignments: [], remaining: required_qty, totals: { cost_cents: 0, penalty_cents: 0, grand_cents: 0 } },
      route: 'auction'
    };
  }
  const assigns = readSolution(res, offers);
  const sumQty = assigns.reduce((s,a)=>s+a.qty,0);
  const route = sumQty >= required_qty ? (assigns.some(a=>a.source==='bid') && assigns.some(a=>a.source==='listing') ? 'mixed' : (assigns.some(a=>a.source==='bid') ? 'auction' : 'stock')) : 'mixed';
  // 비용 합산(목표에 페널티 포함했으므로 penalty 별도 0으로 처리, grand=objective)
  return {
    input: { brand, code, required_qty, due_date },
    offers_count: offers.length,
    solver: 'ilp',
    feasible: true,
    plan: {
      assignments: assigns,
      remaining: Math.max(0, required_qty - sumQty),
      totals: { cost_cents: Math.round(res.result), penalty_cents: 0, grand_cents: Math.round(res.result) }
    },
    route
  };
}

async function ilpOptimize({ items=[], allow_alternatives=true, k_alternatives=6, use_bids=true, lead_penalty_cents_per_unit_per_day=10, alternative_penalty_cents_per_unit=0 } = {}) {
  const out = [];
  let summary = { total_lines: items.length, total_required_qty: 0, total_grand_cents: 0, lines_fully_satisfied: 0, lines_need_pr: 0 };
  for (const it of items) {
    summary.total_required_qty += Number(it.required_qty || 0);
    const r = await ilpOptimizeLine(it, { allow_alternatives, k_alternatives, use_bids, lead_penalty_cents_per_unit_per_day, alternative_penalty_cents_per_unit });
    out.push(r);
    summary.total_grand_cents += r.plan.totals.grand_cents;
    if (r.plan.remaining > 0 || !r.feasible) summary.lines_need_pr += 1; else summary.lines_fully_satisfied += 1;
  }
  return { summary, items: out };
}

module.exports = { ilpOptimize, ilpOptimizeLine };
