const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const db = require('./src/utils/db');
const { sendEmail, sendWebhook } = require('./src/notify/sender');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '8mb' }));

async function fetchBatch(limit=50){
  const r = await db.query(`
    UPDATE public.event_queue SET status='processing', attempts=attempts+1
    WHERE id IN (
      SELECT id FROM public.event_queue WHERE status='queued' AND run_at <= now() ORDER BY run_at, created_at LIMIT $1
    )
    RETURNING *;
  `, [Math.max(1, Math.min(100, limit))]);
  return r.rows;
}

async function selectWinner(prId){
  // 가장 낮은 price_cents, 동점이면 lead_time_days 짧은 것
  const bids = await db.query(`SELECT * FROM public.bids WHERE purchase_request_id=$1 AND status='active' ORDER BY price_cents ASC NULLS LAST, lead_time_days ASC NULLS LAST, created_at ASC`, [prId]);
  return bids.rows[0] || null;
}

async function handle(ev){
  const t = ev.type, p = ev.payload || {};
  if (t === 'rfq_created') {
    const subs = await db.query(`SELECT * FROM public.subscriptions WHERE family_slug=$1 AND active=TRUE`, [p.family_slug || '']);
    for (const s of subs.rows) {
      if (s.target_email) await db.query(`INSERT INTO public.notifications(type, payload, target, channel) VALUES ('rfq_created',$1,$2,'email')`, [p, s.target_email]);
      if (s.target_webhook) await db.query(`INSERT INTO public.notifications(type, payload, target, channel) VALUES ('rfq_created',$1,$2,'webhook')`, [p, s.target_webhook]);
    }
    return { ok: true, fanout: subs.rows.length };
  }
  if (t === 'bid_submitted') {
    // buyer에게 전달(단순화: purchase_requests에 buyer_id가 있다고 가정)
    const pr = await db.query(`SELECT buyer_id FROM public.purchase_requests WHERE id=$1`, [p.purchase_request_id || null]);
    const buyer = pr.rows[0]?.buyer_id || null;
    if (buyer) await db.query(`INSERT INTO public.notifications(type, payload, target, channel) VALUES ('bid_submitted',$1,$2,'email')`, [p, buyer]);
    return { ok: true };
  }
  if (t === 'rfq_deadline_due') {
    const winner = await selectWinner(p.purchase_request_id);
    if (winner) {
      await db.query(`UPDATE public.bids SET status='awarded' WHERE id=$1`, [winner.id]);
      await db.query(`UPDATE public.purchase_requests SET awarded_qty = LEAST(requested_qty, awarded_qty + $2) WHERE id=$1`, [p.purchase_request_id, winner.qty || 0]);
      // 알림
      await db.query(`INSERT INTO public.notifications(type, payload, target, channel) VALUES ('rfq_awarded',$1,$2,'email')`, [{ purchase_request_id: p.purchase_request_id, bid_id: winner.id }, winner.seller_id || 'unknown']);
    }
    return { ok: true, winner: winner?.id || null };
  }
  if (t === 'notify_deliver') {
    const n = p.notification || {};
    if (n.channel === 'email') {
      const out = await sendEmail(n.target, `[RFQ] ${n.type}`, JSON.stringify(n.payload));
      return { ok: out.ok, error: out.error || null };
    } else if (n.channel === 'webhook') {
      const out = await sendWebhook(n.target, n.payload);
      return { ok: out.ok, error: out.error || null };
    }
    return { ok: false, error: 'unknown channel' };
  }
  return { ok: false, error: 'unknown event' };
}

app.post('/api/tasks/process-notify', async (req, res) => {
  try {
    // 1) notifications → deliver events
    const rows = await db.query(`SELECT * FROM public.notifications WHERE status='queued' ORDER BY created_at ASC LIMIT 50`);
    for (const n of rows.rows) {
      await db.query(`INSERT INTO public.event_queue(type, payload) VALUES ('notify_deliver', $1)`, [ { notification: n } ]);
      await db.query(`UPDATE public.notifications SET status='sent' WHERE id=$1`, [n.id]);
    }
    // 2) deliver events + others
    const batch = await fetchBatch(50);
    const results = [];
    for (const ev of batch) {
      try {
        const out = await handle(ev);
        if (out.ok) await db.query(`UPDATE public.event_queue SET status='done', last_error=NULL WHERE id=$1`, [ev.id]);
        else await db.query(`UPDATE public.event_queue SET status='error', last_error=$2 WHERE id=$1`, [ev.id, out.error || 'error']);
        results.push({ id: ev.id, type: ev.type, ok: out.ok });
      } catch (e) {
        await db.query(`UPDATE public.event_queue SET status='error', last_error=$2 WHERE id=$1`, [ev.id, String(e.message || e)]);
      }
    }
    res.json({ ok: true, processed: results.length });
  } catch (e) { res.status(500).json({ error: String(e.message || e) }); }
});

module.exports = app;
