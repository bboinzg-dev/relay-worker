'use strict';

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { getPool } = require('./db');
const { parseActor } = require('./src/utils/auth');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

const pool = getPool();

function pick(h, k) { return h[k] || h[k.toLowerCase()] || h[k.toUpperCase()] || undefined; }
function getTenant(req) {
  const t = pick(req.headers || {}, 'x-actor-tenant') || null;
  return t;
}

async function ensureTables() {
  try {
    await pool.query('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.purchase_plans (
        id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        buyer_user_id text,
        period_month char(7) NOT NULL,
        title text,
        notes text,
        created_at timestamptz DEFAULT now(),
        updated_at timestamptz DEFAULT now(),
        UNIQUE (buyer_user_id, period_month)
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.purchase_plan_items (
        id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        plan_id uuid REFERENCES public.purchase_plans(id) ON DELETE CASCADE,
        manufacturer text NOT NULL,
        part_number text NOT NULL,
        category text,
        required_qty integer NOT NULL DEFAULT 0,
        moq integer,
        quote_deadline date,
        delivery_deadline date,
        notes text,
        created_at timestamptz DEFAULT now(),
        updated_at timestamptz DEFAULT now()
      )
    `);
    await pool.query(`
      ALTER TABLE public.purchase_plan_items
      ADD COLUMN IF NOT EXISTS notes text
    `);
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_purchase_plan_items_plan
        ON public.purchase_plan_items(plan_id)
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.bom_lists (
        id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        owner_user_id text,
        name text NOT NULL,
        note text,
        created_at timestamptz DEFAULT now(),
        updated_at timestamptz DEFAULT now()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.bom_items (
        id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        bom_id uuid REFERENCES public.bom_lists(id) ON DELETE CASCADE,
        row_no integer,
        manufacturer text,
        part_number text,
        qty integer,
        moq integer,
        lead_time_weeks integer,
        note text,
        component_family text,
        created_at timestamptz DEFAULT now(),
        updated_at timestamptz DEFAULT now()
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS public.plan_item_pr_links (
        plan_item_id uuid REFERENCES public.purchase_plan_items(id) ON DELETE CASCADE,
        purchase_request_id uuid REFERENCES public.purchase_requests(id) ON DELETE CASCADE,
        created_at timestamptz DEFAULT now(),
        PRIMARY KEY (plan_item_id, purchase_request_id)
      )
    `);
  } catch (err) {
    console.warn('[plan] ensure tables skipped:', err?.message || err);
  }
}

ensureTables().catch((err) => {
  console.warn('[plan] ensureTables error:', err?.message || err);
});

function normalizeMonth(raw) {
  const now = new Date();
  if (!raw) {
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}`;
  }
  const s = String(raw).trim();
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(s)) {
    throw new Error('month must be YYYY-MM');
  }
  return s;
}

async function ensurePlan(dbClient, buyerId, month, opts = {}) {
  const title = Object.prototype.hasOwnProperty.call(opts, 'title') ? opts.title : undefined;
  const notes = Object.prototype.hasOwnProperty.call(opts, 'notes') ? opts.notes : undefined;
  const values = [
    buyerId,
    month,
    title === undefined ? null : title,
    notes === undefined ? null : notes,
  ];
  const q = await dbClient.query(`
    INSERT INTO public.purchase_plans (buyer_user_id, period_month, title, notes)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (buyer_user_id, period_month)
    DO UPDATE SET
      title = COALESCE(EXCLUDED.title, public.purchase_plans.title),
      notes = COALESCE(EXCLUDED.notes, public.purchase_plans.notes),
      updated_at = now()
    RETURNING *
  `, values);
  return q.rows[0];
}

function coerceInt(value, fallback = null) {
  if (value === null || value === undefined || value === '') return fallback;
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
}

app.get('/api/purchase-plans', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) return res.status(401).json({ error: 'auth required' });
    const month = normalizeMonth(req.query.month);
    const plan = await ensurePlan(pool, actor.id, month);
    const itemsRes = await pool.query(`
      SELECT *
        FROM public.purchase_plan_items
       WHERE plan_id = $1
       ORDER BY created_at ASC
    `, [plan.id]);
    const items = itemsRes.rows;
    let prMap = new Map();
    if (items.length) {
      const itemIds = items.map((i) => i.id);
      const links = await pool.query(`
        SELECT plan_item_id, purchase_request_id
          FROM public.plan_item_pr_links
         WHERE plan_item_id = ANY($1::uuid[])
      `, [itemIds]);
      const prIds = links.rows.map((r) => r.purchase_request_id);
      let prRows = [];
      if (prIds.length) {
        const prs = await pool.query(`
          SELECT *
            FROM public.purchase_requests
           WHERE id = ANY($1::uuid[])
        `, [prIds]);
        prRows = prs.rows;
      }
      const prById = new Map(prRows.map((r) => [r.id, r]));
      prMap = new Map(itemIds.map((id) => [id, []]));
      for (const link of links.rows) {
        const arr = prMap.get(link.plan_item_id) || [];
        const pr = prById.get(link.purchase_request_id);
        if (pr) arr.push(pr);
        prMap.set(link.plan_item_id, arr);
      }
    }
    const enriched = items.map((item) => ({
      ...item,
      purchase_requests: prMap.get(item.id) || [],
    }));
    res.json({ ok: true, plan, items: enriched });
  } catch (err) {
    console.error(err);
    res.status(400).json({ ok: false, error: err?.message || String(err) });
  }
});

app.post('/api/purchase-plans', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) return res.status(401).json({ error: 'auth required' });
    const raw = req.body || {};
    const body = raw && raw.item ? {
      month: raw.month,
      manufacturer: raw.item.manufacturer,
      part_number: raw.item.partNumber,
      category: raw.item.category,
      required_qty: raw.item.requiredQty,
      moq: raw.item.moq,
      quote_deadline: raw.item.quoteDeadline,
      delivery_deadline: raw.item.deliveryDeadline,
      plan_title: raw.plan_title || raw.title,
      plan_notes: raw.plan_notes || raw.notes,
    } : raw;
    const month = normalizeMonth(body.period_month || body.month);
    if (!body.manufacturer || !body.part_number) {
      return res.status(400).json({ error: 'manufacturer & part_number required' });
    }
    const plan = await ensurePlan(pool, actor.id, month, {
      title: body.plan_title || body.title || null,
      notes: body.plan_notes || body.notes || null,
    });
    const itemRes = await pool.query(`
      INSERT INTO public.purchase_plan_items
        (plan_id, manufacturer, part_number, category, required_qty, moq, quote_deadline, delivery_deadline)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING *
    `, [
      plan.id,
      body.manufacturer,
      body.part_number,
      body.category || null,
      coerceInt(body.required_qty, 0),
      coerceInt(body.moq, null),
      body.quote_deadline || null,
      body.delivery_deadline || null,
    ]);
    await pool.query(`UPDATE public.purchase_plans SET updated_at = now() WHERE id = $1`, [plan.id]);
    res.json({ ok: true, plan, item: itemRes.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(400).json({ ok: false, error: err?.message || String(err) });
  }
});

app.post('/api/purchase-plans/:planId/items', async (req, res) => {
  const actor = parseActor(req);
  if (!actor?.id) {
    return res.status(401).json({ error: 'auth required' });
  }

  const planId = String(req.params.planId || '').trim();
  if (!planId) {
    return res.status(400).json({ error: 'plan_id required' });
  }

  const body = req.body || {};
  const manufacturer = (body.manufacturer || body.brand || '').toString().trim();
  const partNumber = (body.part_number || body.partNumber || '').toString().trim();
  if (!manufacturer || !partNumber) {
    return res.status(400).json({ error: 'manufacturer & part_number required' });
  }

  const category = body.category || null;
  const requiredQty = coerceInt(body.required_qty ?? body.requiredQty, 0);
  const quoteDeadline = body.quote_deadline || body.quoteDeadline || null;
  const deliveryDeadline = body.delivery_deadline || body.deliveryDeadline || null;
  const notes = body.notes || body.note || null;
  const openRfQ = body.open_rfq === undefined || body.open_rfq === null ? true : !!body.open_rfq;
  const tenantId = getTenant(req) || actor.tenantId || null;

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const plan = await client.query(`
      SELECT id
        FROM public.purchase_plans
       WHERE id = $1 AND buyer_user_id = $2
       FOR UPDATE
    `, [planId, actor.id]);
    if (!plan.rows.length) {
      throw new Error('plan_not_found');
    }

    const inserted = await client.query(`
      INSERT INTO public.purchase_plan_items
        (plan_id, manufacturer, part_number, category, required_qty, quote_deadline, delivery_deadline, notes)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING *
    `, [
      planId,
      manufacturer,
      partNumber,
      category || null,
      requiredQty,
      quoteDeadline || null,
      deliveryDeadline || null,
      notes || null,
    ]);

    const item = inserted.rows[0];
    let purchaseRequestId = null;

    if (openRfQ) {
      const qty = coerceInt(body.offer_qty ?? body.offerQty, requiredQty) || requiredQty || 0;
      const summary = `${manufacturer} ${partNumber}`.trim();
      const createArgs = [
        tenantId,
        actor.id,
        manufacturer,
        partNumber,
        qty,
        deliveryDeadline ? new Date(deliveryDeadline) : null,
        quoteDeadline ? new Date(quoteDeadline) : null,
        summary || null,
        notes || null,
        category || null,
      ];

      let prId = null;
      try {
        const pr = await client.query(`
          SELECT public.create_purchase_request(
            $1::text, $2::text, $3::text, $4::text, $5::int,
            $6::timestamptz, $7::timestamptz, true,
            $8::text, $9::text,
            '[]'::jsonb, jsonb_build_object('category', $10::text)
          ) AS id
        `, createArgs);
        prId = pr.rows[0]?.id || null;
      } catch (err) {
        try {
          const pr = await client.query(`
            INSERT INTO public.purchase_requests
              (tenant_id, buyer_id, brand, code, qty_required, need_by_date, bid_deadline_at, notes, status, extra)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'open', jsonb_build_object('category', $9))
            RETURNING id
          `, [
            tenantId,
            actor.id,
            manufacturer,
            partNumber,
            qty,
            deliveryDeadline || null,
            quoteDeadline || null,
            notes || null,
            category || null,
          ]);
          prId = pr.rows[0]?.id || null;
        } catch (fallbackErr) {
          const pr = await client.query(`
            INSERT INTO public.purchase_requests
              (tenant_id, buyer_id, brand, code, qty_required, need_by_date, notes, status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,'open')
            RETURNING id
          `, [
            tenantId,
            actor.id,
            manufacturer,
            partNumber,
            qty,
            deliveryDeadline || null,
            notes || null,
          ]);
          prId = pr.rows[0]?.id || null;
        }
      }

      if (prId) {
        purchaseRequestId = prId;
        await client.query(`
          INSERT INTO public.plan_item_pr_links (plan_item_id, purchase_request_id)
          VALUES ($1,$2)
          ON CONFLICT DO NOTHING
        `, [item.id, prId]);
      }
    }

    await client.query('UPDATE public.purchase_plan_items SET updated_at = now() WHERE id = $1', [item.id]);
    await client.query('UPDATE public.purchase_plans SET updated_at = now() WHERE id = $1', [planId]);
    await client.query('COMMIT');

    return res.json({
      ok: true,
      plan_item: item,
      purchase_request_id: purchaseRequestId,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('[plan] create item failed:', err?.message || err);
    const msg = err?.message === 'plan_not_found' ? 'plan_not_found' : err?.message || 'internal_error';
    return res.status(err?.message === 'plan_not_found' ? 404 : 400).json({ ok: false, error: msg });
  } finally {
    client.release();
  }
});

app.delete('/api/purchase-plans/items/:id', async (req, res) => {
  try {
    const actor = parseActor(req);
    const userId = actor?.user_id ?? actor?.id;
    if (!userId) return res.status(401).json({ ok: false, error: 'auth required' });

    const itemId = (req.params.id || '').toString();
    if (!itemId) return res.status(400).json({ ok: false, error: 'id required' });

    await pool.query(`
      UPDATE public.purchase_requests
         SET status = CASE WHEN status = 'open' THEN 'cancelled' ELSE status END,
             updated_at = now()
       WHERE id IN (
         SELECT purchase_request_id FROM public.plan_item_pr_links WHERE plan_item_id = $1
       )
    `, [itemId]);

    const result = await pool.query(`
      WITH victim AS (
        SELECT i.id, i.plan_id
          FROM public.purchase_plan_items i
          JOIN public.purchase_plans p ON p.id = i.plan_id
         WHERE i.id = $1
           AND p.buyer_user_id = $2
      )
      DELETE FROM public.purchase_plan_items i
      USING victim v
      WHERE i.id = v.id
      RETURNING i.id, i.plan_id
    `, [itemId, userId]);

    if (!result.rowCount) {
      return res.status(404).json({ ok: false, error: 'not found' });
    }

    const deleted = result.rows[0];
    if (deleted?.plan_id) {
      await pool.query('UPDATE public.purchase_plans SET updated_at = now() WHERE id = $1', [deleted.plan_id]);
    }

    res.json({ ok: true, id: deleted.id });
  } catch (err) {
    console.error(err);
    res.status(400).json({ ok: false, error: err?.message || String(err) });
  }
});

app.post('/api/purchase-plans/from-bom', async (req, res) => {
  const client = await pool.connect();
  try {
    const actor = parseActor(req);
    if (!actor?.id) {
      client.release();
      return res.status(401).json({ error: 'auth required' });
    }
    const body = req.body || {};
    const bomId = (body.bom_id || body.bomId || '').toString();
    if (!bomId) {
      client.release();
      return res.status(400).json({ error: 'bom_id required' });
    }
    const month = normalizeMonth(body.period_month || body.month);
    await client.query('BEGIN');
    const bom = await client.query(`
      SELECT * FROM public.bom_lists WHERE id = $1 AND owner_user_id = $2
    `, [bomId, actor.id]);
    if (!bom.rows.length) throw new Error('bom not found');
    const plan = await ensurePlan(client, actor.id, month, {
      title: body.plan_title || body.title || null,
      notes: body.plan_notes || body.notes || null,
    });
    const items = await client.query(`
      SELECT * FROM public.bom_items WHERE bom_id = $1 ORDER BY COALESCE(row_no, 0) ASC, created_at ASC
    `, [bomId]);
    if (!items.rows.length) {
      await client.query('COMMIT');
      client.release();
      return res.json({ ok: true, plan, inserted: [] });
    }
    const inserted = await client.query(`
      INSERT INTO public.purchase_plan_items
        (plan_id, manufacturer, part_number, category, required_qty, moq, quote_deadline, delivery_deadline)
      SELECT $1, manufacturer, part_number, COALESCE(component_family, $2::text), COALESCE(qty, 0), moq, $3::date, $4::date
        FROM public.bom_items
       WHERE bom_id = $5
         AND manufacturer IS NOT NULL
         AND part_number IS NOT NULL
      RETURNING *
    `, [
      plan.id,
      body.category || null,
      body.quote_deadline || null,
      body.delivery_deadline || null,
      bomId,
    ]);
    await client.query(`UPDATE public.purchase_plans SET updated_at = now() WHERE id = $1`, [plan.id]);
    await client.query('COMMIT');
    client.release();
    res.json({ ok: true, plan, inserted: inserted.rows });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    client.release();
    console.error(err);
    res.status(400).json({ ok: false, error: err?.message || String(err) });
  }
});

app.post('/api/purchase-plans/items/:id/rfq', async (req, res) => {
  const client = await pool.connect();
  try {
    const actor = parseActor(req);
    if (!actor?.id) {
      client.release();
      return res.status(401).json({ error: 'auth required' });
    }
    const itemId = (req.params.id || '').toString();
    if (!itemId) {
      client.release();
      return res.status(400).json({ error: 'id required' });
    }
    const tenant = getTenant(req);
    const body = req.body || {};
    await client.query('BEGIN');
    const itemRes = await client.query(`
      SELECT i.*, p.buyer_user_id
        FROM public.purchase_plan_items i
        JOIN public.purchase_plans p ON p.id = i.plan_id
       WHERE i.id = $1 AND p.buyer_user_id = $2
       FOR UPDATE
    `, [itemId, actor.id]);
    if (!itemRes.rows.length) throw new Error('plan item not found');
    const item = itemRes.rows[0];
    const qty = coerceInt(body.qty || body.qty_required, item.required_qty || 0);
    if (!qty || qty <= 0) throw new Error('qty_required must be > 0');
    const needBy = body.need_by_date
      || body.needByDate
      || item.delivery_deadline
      || null;
    const bidDeadline = body.bid_deadline_at
      || body.bidDeadlineAt
      || body.quote_deadline
      || body.quoteDeadline
      || item.quote_deadline
      || null;
    const targetPrice = body.target_unit_price_cents || null;
    const allowSubsRaw = body.allow_substitutes;
    const allowSubs = allowSubsRaw === undefined || allowSubsRaw === null ? null : !!allowSubsRaw;
    const notes = body.notes || body.note || null;
    const rfqTitle = `${item.manufacturer || ''} ${item.part_number || ''}`.trim() || null;
    const pr = await client.query(`
      INSERT INTO public.purchase_requests
        (tenant_id, buyer_id, brand, code, qty_required, need_by_date, bid_deadline_at, target_unit_price_cents, allow_substitutes, notes, rfq_title, status)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,COALESCE($9,true),$10,$11,'open')
      RETURNING *
    `, [
      tenant,
      actor.id || null,
      item.manufacturer,
      item.part_number,
      qty,
      needBy || null,
      bidDeadline || null,
      targetPrice || null,
      allowSubs,
      notes,
      rfqTitle,
    ]);
    const prRow = pr.rows[0];
    await client.query(`
      INSERT INTO public.plan_item_pr_links (plan_item_id, purchase_request_id)
      VALUES ($1,$2)
      ON CONFLICT DO NOTHING
    `, [itemId, prRow.id]);
    await client.query(`UPDATE public.purchase_plan_items SET updated_at = now() WHERE id = $1`, [itemId]);
    await client.query(`UPDATE public.purchase_plans SET updated_at = now() WHERE id = $1`, [item.plan_id]);
    await client.query('COMMIT');
    client.release();
    res.json({ ok: true, purchase_request: prRow });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {}
    client.release();
    console.error(err);
    res.status(400).json({ ok: false, error: err?.message || String(err) });
  }
});

app.get('/api/purchase-plans/items/:id/bids', async (req, res) => {
  try {
    const actor = parseActor(req);
    if (!actor?.id) return res.status(401).json({ error: 'auth required' });
    const itemId = (req.params.id || '').toString();
    if (!itemId) return res.status(400).json({ error: 'id required' });
    const item = await pool.query(`
      SELECT i.id
        FROM public.purchase_plan_items i
        JOIN public.purchase_plans p ON p.id = i.plan_id
       WHERE i.id = $1 AND p.buyer_user_id = $2
    `, [itemId, actor.id]);
    if (!item.rows.length) return res.status(404).json({ error: 'item not found' });
    const links = await pool.query(`
      SELECT purchase_request_id
        FROM public.plan_item_pr_links
       WHERE plan_item_id = $1
    `, [itemId]);
    if (!links.rows.length) return res.json({ ok: true, purchase_requests: [] });
    const prIds = links.rows.map((r) => r.purchase_request_id);
    const prRows = await pool.query(`
      SELECT * FROM public.purchase_requests WHERE id = ANY($1::uuid[])
    `, [prIds]);
    const bidsRows = await pool.query(`
      SELECT pb.*, pb.offer_qty AS qty_offer
        FROM public.plan_bids pb
       WHERE pb.purchase_request_id = ANY($1::uuid[])
       ORDER BY pb.created_at DESC
    `, [prIds]);
    const bidsByPr = new Map();
    for (const bid of bidsRows.rows) {
      const arr = bidsByPr.get(bid.purchase_request_id) || [];
      arr.push(bid);
      bidsByPr.set(bid.purchase_request_id, arr);
    }
    const data = prRows.rows.map((pr) => ({
      ...pr,
      bids: bidsByPr.get(pr.id) || [],
    }));
    res.json({ ok: true, purchase_requests: data });
  } catch (err) {
    console.error(err);
    res.status(400).json({ ok: false, error: err?.message || String(err) });
  }
});

module.exports = app;