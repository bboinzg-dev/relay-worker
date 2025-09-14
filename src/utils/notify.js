const db = require('./db');

async function ensureEventsTable() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.events (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      type text NOT NULL,
      tenant_id uuid,
      actor_id uuid,
      targets uuid[],
      payload jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    );
  `);
}

async function getKnownSpecsTables() {
  const r = await db.query(`SELECT specs_table FROM public.component_registry ORDER BY specs_table`);
  return r.rows.map(x => x.specs_table);
}

async function findFamilyForBrandCode(brand, code) {
  const tables = await getKnownSpecsTables();
  const bn = brand.toLowerCase(), cn = code.toLowerCase();
  for (const t of tables) {
    const q = await db.query(`SELECT family_slug FROM public/${t} WHERE brand_norm=$1 AND code_norm=$2 LIMIT 1`, [bn, cn]);
    if (q.rows.length) return q.rows[0].family_slug;
  }
  // fallback: try relay_specs
  const q2 = await db.query(`SELECT family_slug FROM public.relay_specs WHERE brand_norm=$1 AND code_norm=$2 LIMIT 1`, [bn, cn]);
  if (q2.rows.length) return q2.rows[0].family_slug || 'relay';
  return null;
}

async function findSellerRecipients({ family_slug=null, brand=null, code=null }) {
  const params = [];
  let where = [];
  if (family_slug) { params.push(family_slug); where.push('(s.family_slug = $'+params.length+' OR s.family_slug IS NULL)'); }
  if (brand) { params.push(brand.toLowerCase()); where.push('(s.brand_norm = $'+params.length+' OR s.brand_norm IS NULL)'); }
  if (code) { params.push(code.toLowerCase()); where.push('(s.code_norm = $'+params.length+' OR s.code_norm IS NULL)'); }
  const sql = `
    SELECT a.id, a.email, a.display_name, a.webhook_url
    FROM public.accounts a
    JOIN public.seller_subscriptions s ON s.seller_id = a.id
    WHERE a.role = 'seller' ${where.length ? 'AND ' + where.join(' AND ') : ''}
  `;
  const r = await db.query(sql, params);
  return r.rows;
}

async function recordEvent({ type, tenant_id=null, actor_id=null, targets=[], payload={} }) {
  await ensureEventsTable();
  const row = await db.query(
    `INSERT INTO public.events (type, tenant_id, actor_id, targets, payload)
     VALUES ($1,$2,$3,$4,$5) RETURNING *`,
    [type, tenant_id, actor_id, targets, payload]
  );
  return row.rows[0];
}

async function deliverWebhook(url, body, { timeoutMs=2000 } = {}) {
  if (!url) return { ok: false, skipped: true };
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    clearTimeout(t);
    return { ok: resp.ok, status: resp.status };
  } catch (e) {
    clearTimeout(t);
    return { ok: false, error: String(e.message || e) };
  }
}

async function notify(type, { tenant_id=null, actor_id=null, family_slug=null, brand=null, code=null, data={} }) {
  const recips = await findSellerRecipients({ family_slug, brand, code });
  const evt = await recordEvent({ type, tenant_id, actor_id, targets: recips.map(r => r.id), payload: { family_slug, brand, code, data } });
  const results = [];
  for (const r of recips) {
    const body = { event_id: evt.id, type, family_slug, brand, code, data };
    const out = await deliverWebhook(r.webhook_url, body, { timeoutMs: Number(process.env.NOTIFY_WEBHOOK_TIMEOUT_MS || 2000) });
    results.push({ account_id: r.id, result: out });
  }
  return { event: evt, deliveries: results };
}

module.exports = { notify, findFamilyForBrandCode, recordEvent };
