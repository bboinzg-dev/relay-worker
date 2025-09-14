const db = require('../utils/db');

async function markInvoicePaid(invoice_id, { payment_id=null } = {}){
  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');
    const inv = await client.query(`SELECT * FROM public.invoices WHERE id=$1 FOR UPDATE`, [invoice_id]);
    if (!inv.rows.length) throw new Error('invoice not found');
    if (inv.rows[0].status === 'paid') { await client.query('COMMIT'); return { already:true }; }
    const orderId = inv.rows[0].order_id;
    const items = await client.query(`SELECT * FROM public.order_items WHERE order_id=$1 ORDER BY id FOR UPDATE`, [orderId]);

    // 차감
    for (const it of items.rows) {
      if (it.listing_id) {
        const u = await client.query(`
          UPDATE public.listings SET quantity_available = quantity_available - $1
          WHERE id=$2 AND quantity_available >= $1
          RETURNING id`, [it.qty, it.listing_id]);
        if (!u.rows.length) throw new Error('insufficient stock for listing ' + it.listing_id);
      }
    }

    await client.query(`UPDATE public.invoices SET status='paid', paid_at=now() WHERE id=$1`, [invoice_id]);
    await client.query(`UPDATE public.orders SET status='paid' WHERE id=$1`, [orderId]);
    if (payment_id) await client.query(`UPDATE public.payments SET status='captured' WHERE id=$1`, [payment_id]);
    await client.query('COMMIT');
    return { ok: true };
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch{}
    throw e;
  } finally {
    client.release();
  }
}

module.exports = { markInvoicePaid };
