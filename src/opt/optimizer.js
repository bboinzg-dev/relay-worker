const db = require('../utils/db');

function daysUntil(due) {
  if (!due) return null;
  try {
    const dueDate = new Date(due);
    const now = new Date();
    return Math.ceil((dueDate.getTime() - now.getTime()) / 86400000);
  } catch { return null; }
}

async function ensureExt() {
  try { await db.query('CREATE EXTENSION IF NOT EXISTS pg_trgm;'); } catch {}
  try { await db.query('CREATE EXTENSION IF NOT EXISTS vector;'); } catch {}
}

async function getKnownSpecsTables() {
  const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`);
  return r.rows;
}

async function findExactRow(brand, code) {
  const regs = await getKnownSpecsTables();
  const bn = brand.toLowerCase();
  const cn = code.toLowerCase();
  for (const r of regs) {
    const q = await db.query(`
      SELECT * FROM public/${r.specs_table}
      WHERE (brand_norm=lower($1) OR lower(brand)=lower($1))
        AND (code_norm=lower($2) OR lower(code)=lower($2))
      LIMIT 1
    `, [brand, code]);
    if (q.rows.length) return { table: r.specs_table, family_slug: q.rows[0].family_slug || r.family_slug, row: q.rows[0] };
  }
  return null;
}

async function getAlternativesFor(table, baseRow, k=8) {
  // try embedding first
  try {
    if (baseRow.embedding) {
      const q = await db.query(`
        SELECT *, (embedding <=> $1::vector) AS dist
        FROM public/${table}
        WHERE NOT (brand_norm=$2 AND code_norm=$3)
        ORDER BY embedding <=> $1::vector
        LIMIT $4
      `, [baseRow.embedding, baseRow.brand_norm || baseRow.brand?.toLowerCase(), baseRow.code_norm || baseRow.code?.toLowerCase(), k]);
      return { mode: 'embedding', items: q.rows };
    }
  } catch (e) {
    // ignore if embedding/extension not present
  }
  // fallback: rule-based
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
    source: 'bid',
    id: x.id,
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
    source: 'bid',
    id: x.id,
    brand: x.alt_brand, code: x.alt_code,
    is_alternative: true,
    unit_price_cents: Number(x.price_cents || 0),
    currency: x.currency || 'USD',
    available_qty: Number(x.offer_qty || 0),
    lead_time_days: x.lead_time_days == null ? null : Number(x.lead_time_days),
    meta: { purchase_request_id: x.purchase_request_id }
  }));
}

function computeEffectiveUnit(offer, { due_date=null, lead_penalty_cents_per_unit_per_day=10, alternative_penalty_cents_per_unit=0 }) {
  let leadPenalty = 0;
  const daysLeft = daysUntil(due_date);
  if (daysLeft != null && offer.lead_time_days != null) {
    const delay = Math.max(0, offer.lead_time_days - daysLeft);
    leadPenalty = delay * Number(lead_penalty_cents_per_unit_per_day || 0);
  }
  const altPen = offer.is_alternative ? Number(alternative_penalty_cents_per_unit || 0) : 0;
  return {
    leadPenaltyCents: leadPenalty,
    altPenaltyCents: altPen,
    effectiveUnitCents: Number(offer.unit_price_cents || 0) + leadPenalty + altPen
  };
}

function greedyAllocate(required_qty, offers, penaltyCfg) {
  const ranked = offers.slice().map(o => {
    const m = computeEffectiveUnit(o, penaltyCfg);
    return { ...o, _metrics: m };
  }).sort((a,b) => a._metrics.effectiveUnitCents - b._metrics.effectiveUnitCents);

  const plan = [];
  let remaining = Number(required_qty || 0);
  let totalCost = 0, totalPenalty = 0;
  for (const o of ranked) {
    if (remaining <= 0) break;
    const take = Math.min(remaining, Number(o.available_qty || 0));
    if (take <= 0) continue;
    const unit = Number(o.unit_price_cents || 0);
    const eff = o._metrics.effectiveUnitCents;
    const leadPen = o._metrics.leadPenaltyCents;
    const altPen = o._metrics.altPenaltyCents;
    plan.push({
      source: o.source, offer_id: o.id,
      brand: o.brand, code: o.code,
      qty: take,
      unit_price_cents: unit,
      effective_unit_cents: eff,
      lead_time_days: o.lead_time_days,
      is_alternative: o.is_alternative,
      currency: o.currency || 'USD',
      penalties_per_unit: { lead_cents: leadPen, alternative_cents: altPen },
      meta: o.meta || {}
    });
    totalCost += take * unit;
    totalPenalty += take * (leadPen + altPen);
    remaining -= take;
  }
  return { assignments: plan, remaining, totals: { cost_cents: Math.round(totalCost), penalty_cents: Math.round(totalPenalty), grand_cents: Math.round(totalCost + totalPenalty) } };
}

function classifyRoute(plan) {
  const usedListings = plan.assignments.some(a => a.source === 'listing');
  const usedBids = plan.assignments.some(a => a.source === 'bid');
  if (plan.remaining > 0) return usedListings ? 'mixed' : 'auction';
  if (usedBids && usedListings) return 'mixed';
  if (usedBids) return 'auction';
  return 'stock';
}

async function optimizeLine({ brand, code, required_qty, due_date=null }, { allow_alternatives=true, k_alternatives=6, use_bids=true, lead_penalty_cents_per_unit_per_day=10, alternative_penalty_cents_per_unit=0 } = {}) {
  await ensureExt();
  const offers = [];
  // direct sku offers
  offers.push(...await gatherListings(brand, code));
  if (use_bids) offers.push(...await gatherBidsForSku(brand, code));

  if (allow_alternatives) {
    const base = await findExactRow(brand, code);
    if (base) {
      const alts = await getAlternativesFor(base.table, base.row, k_alternatives);
      for (const r of alts.items) {
        // listings for alternatives
        offers.push(...(await gatherListings(r.brand || r.brand_norm, r.code || r.code_norm)).map(x => ({ ...x, is_alternative: true })));
        // alternative bids that point to this alt brand/code
        if (use_bids) offers.push(...(await gatherAltBidsFor(r.brand || r.brand_norm, r.code || r.code_norm)));
      }
    }
  }

  // normalize
  const penaltyCfg = { due_date, lead_penalty_cents_per_unit_per_day, alternative_penalty_cents_per_unit };
  const plan = greedyAllocate(required_qty, offers, penaltyCfg);
  const route = classifyRoute(plan);
  return {
    input: { brand, code, required_qty, due_date },
    options: { allow_alternatives, k_alternatives, use_bids, lead_penalty_cents_per_unit_per_day, alternative_penalty_cents_per_unit },
    offers_count: offers.length,
    route,
    plan
  };
}

async function optimize({ items=[], allow_alternatives=true, k_alternatives=6, use_bids=true, lead_penalty_cents_per_unit_per_day=10, alternative_penalty_cents_per_unit=0 } = {}) {
  const out = [];
  let summary = { total_lines: items.length, total_required_qty: 0, total_grand_cents: 0, lines_fully_satisfied: 0, lines_need_pr: 0 };
  for (const it of items) {
    summary.total_required_qty += Number(it.required_qty || 0);
    const r = await optimizeLine(it, { allow_alternatives, k_alternatives, use_bids, lead_penalty_cents_per_unit_per_day, alternative_penalty_cents_per_unit });
    out.push(r);
    summary.total_grand_cents += r.plan.totals.grand_cents;
    if (r.plan.remaining > 0) summary.lines_need_pr += 1; else summary.lines_fully_satisfied += 1;
  }
  return { summary, items: out };
}

module.exports = { optimize, optimizeLine };
