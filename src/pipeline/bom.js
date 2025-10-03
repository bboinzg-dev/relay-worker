const db = require('../../db');
const { updateRowEmbedding } = (()=>{ try { return require('./embedding'); } catch { return { updateRowEmbedding: async()=>false }; } })();
const { notify, findFamilyForBrandCode } = (()=>{ try { return require('../utils/notify'); } catch { return { notify: async()=>({}), findFamilyForBrandCode: async()=>null }; } })();

async function ensureExt() {
  await db.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";');
  try { await db.query('CREATE EXTENSION IF NOT EXISTS vector;'); } catch {}
  try { await db.query('CREATE EXTENSION IF NOT EXISTS pg_trgm;'); } catch {}
}

async function getKnownSpecsTables() {
  const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`);
  return r.rows;
}

async function findExactInTable(table, brand, code) {
  const q = await db.query(`SELECT * FROM public/${table} WHERE brand_norm=lower($1) AND code_norm=lower($2) LIMIT 1`, [brand, code]);
  return q.rows[0] || null;
}

async function findExact(brand, code) {
  const regs = await getKnownSpecsTables();
  for (const r of regs) {
    const row = await findExactInTable(r.specs_table, brand, code);
    if (row) return { table: r.specs_table, family_slug: r.family_slug, row };
  }
  return null;
}

async function findFuzzy(brand, code, limit=5) {
  await ensureExt();
  const regs = await getKnownSpecsTables();
  const out = [];
  for (const r of regs) {
    // brand match + trigram on code/display_name/series
    const q = await db.query(`
      SELECT *, 1.0 - GREATEST(similarity(code_norm, lower($2)), similarity(lower(display_name), lower($2)), similarity(lower(series), lower($2))) AS score
        FROM public/${r.specs_table}
       WHERE brand_norm = lower($1)
       ORDER BY score ASC NULLS LAST
       LIMIT $3
    `, [brand, code, limit]);
    for (const row of q.rows) out.push({ table: r.specs_table, family_slug: r.family_slug, row, score: row.score ?? 0.9 });
  }
  out.sort((a,b)=> (a.score||1)-(b.score||1));
  return out.slice(0, limit);
}

async function getListings(brand, code) {
  const r = await db.query(`SELECT * FROM public.listings WHERE brand_norm=lower($1) AND code_norm=lower($2) AND quantity_available>0 ORDER BY price_cents ASC, created_at DESC`, [brand, code]);
  const items = r.rows;
  const total_available = items.reduce((s,it)=> s + Number(it.quantity_available||0), 0);
  return { items, total_available };
}

async function getAlternatives(table, baseRow, k=8) {
  // embedding first
  try {
    if (!baseRow.embedding) {
      // try to compute one now (best-effort)
      await updateRowEmbedding(table, baseRow);
      const ref = await db.query(`SELECT embedding FROM public/${table} WHERE brand_norm=$1 AND code_norm=$2`, [baseRow.brand_norm, baseRow.code_norm]);
      baseRow.embedding = ref.rows[0]?.embedding || null;
    }
  } catch {}

  if (baseRow.embedding) {
    const q = await db.query(
      `SELECT *, (embedding <=> $1::vector) AS dist
         FROM public/${table}
        WHERE NOT (brand_norm=$2 AND code_norm=$3)
        ORDER BY embedding <=> $1::vector
        LIMIT $4`,
      [baseRow.embedding, baseRow.brand_norm, baseRow.code_norm, k]
    );
    return { mode: 'embedding', items: q.rows };
  }

  // fallback rule-based for known columns
  const q2 = await db.query(
    `SELECT *,
      (CASE WHEN family_slug IS NOT NULL AND family_slug = $1 THEN 0 ELSE 1 END) * 1.0 +
      COALESCE(ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($2::numeric,0)) / 100.0, 1.0) AS score
     FROM public.${table}
     WHERE NOT (brand_norm=$3 AND code_norm=$4)
     ORDER BY score ASC
     LIMIT $5`,
    [baseRow.family_slug || null, baseRow.coil_voltage_vdc || null, baseRow.brand_norm, baseRow.code_norm, k]
  );
  return { mode: 'rule-fallback', items: q2.rows };
}

function buildStockPlan(required, listings) {
  const plan = [];
  let need = required;
  for (const l of listings) {
    if (need <= 0) break;
    const take = Math.min(need, Number(l.quantity_available||0));
    if (take > 0) {
      plan.push({ listing_id: l.id, take_qty: take, price_cents: l.price_cents, currency: l.currency, lead_time_days: l.lead_time_days });
      need -= take;
    }
  }
  return { use_listings: plan, remaining: need };
}

async function analyzeBom({ upload_id=null, rows=null }) {
  await ensureExt();
  let items = [];
  if (upload_id) {
    const r = await db.query(`SELECT brand, code, quantity AS qty, need_by FROM public.bom_lines WHERE upload_id=$1 ORDER BY brand, code`, [upload_id]);
    items = r.rows.map(x => ({ brand: x.brand, code: x.code, qty: Number(x.qty || 0), need_by: x.need_by || null }));
  } else if (Array.isArray(rows)) {
    items = rows.map(x => ({ brand: x.brand, code: x.code, qty: Number(x.qty || 0), need_by: x.need_by || null }));
  } else {
    throw new Error('upload_id or rows[] required');
  }

  const results = [];
  let summary = { total_lines: items.length, lines_exact: 0, total_required_qty: 0, total_available_qty: 0, lines_stock_satisfied: 0, lines_need_pr: 0 };
  for (const it of items) {
    const required = Number(it.qty || 0);
    summary.total_required_qty += required;

    // match exact or fuzzy
    let match = await findExact(it.brand, it.code);
    let matchType = 'exact';
    if (!match) {
      const cands = await findFuzzy(it.brand, it.code, 3);
      matchType = cands.length ? 'fuzzy' : 'unknown';
      if (cands.length) match = { table: cands[0].table, family_slug: cands[0].family_slug, row: cands[0].row, score: cands[0].score };
    } else {
      summary.lines_exact += 1;
    }

    // stock
    let stock = { items: [], total_available: 0 };
    if (match) stock = await getListings(match.row.brand, match.row.code);
    summary.total_available_qty += stock.total_available;

    // alternatives (if not exact or stock 부족)
    let alternatives = null;
    if (match) {
      alternatives = await getAlternatives(match.table, match.row, 8);
    }

    // recommendation
    const plan = buildStockPlan(required, stock.items);
    let route = 'stock';
    if (plan.remaining > 0) {
      route = stock.items.length ? 'mixed' : 'auction';
      summary.lines_need_pr += 1;
    } else {
      summary.lines_stock_satisfied += 1;
    }

    results.push({
      input: it,
      match: match ? { type: matchType, table: match.table, family_slug: match.family_slug, brand: match.row.brand, code: match.row.code, row: match.row, score: match.score || null } : { type: 'unknown' },
      stock: { total_available: stock.total_available, listings: stock.items },
      alternatives,
      recommendation: {
        route,
        plan
      }
    });
  }

  return { summary, items: results };
}

async function persistPlan({ plan_items=[], actor={}, tenant_id=null }) {
  const created = [];
  for (const it of plan_items) {
    const remaining = Number(it.recommendation?.plan?.remaining || 0);
    if (remaining > 0 && it.match && it.match.brand && it.match.code) {
      // create purchase request
      const brand = it.match.brand;
      const code = it.match.code;
      const lead_time_days = null;
      const target_price_cents = null;
      const buyer_ref = 'bom-plan';
      const note = `Auto-created from BOM plan`;
      const due_date = it.input?.need_by || null;

      const id = (await db.query(`INSERT INTO public.purchase_requests (id, brand, code, brand_norm, code_norm, required_qty, lead_time_days, target_price_cents, buyer_ref, note, due_date, owner_id, tenant_id)
                                  VALUES (uuid_generate_v4(), $1,$2, lower($1),lower($2), $3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
                                  [brand, code, remaining, lead_time_days, target_price_cents, buyer_ref, note, due_date, actor.id || null, tenant_id || actor.tenantId || null])).rows[0].id;
      // notify sellers
      try {
        const family = await findFamilyForBrandCode(brand, code);
        await notify('purchase_request.created', { tenant_id: tenant_id || actor.tenantId || null, actor_id: actor.id || null, family_slug: family, brand, code, data: { purchase_request_id: id, required_qty: remaining } });
      } catch (e) { console.warn('notify PR (plan) failed', e.message || e); }
      created.push({ type: 'purchase_request', id, brand, code, required_qty: remaining });
    }
    // placing orders from listings is not executed here — out of scope (manual confirm step preferred).
  }
  return { created };
}

module.exports = { analyzeBom, persistPlan };
