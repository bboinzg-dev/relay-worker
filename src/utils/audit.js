const { parseActor } = require('./auth');
async function writeAudit(db, req, { action, table, row_pk=null, before=null, after=null }){
  try {
    const actor = parseActor(req);
    const ip = (req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '').toString();
    const ua = (req.headers['user-agent'] || '').toString();
    await db.query(`INSERT INTO public.audit_logs(actor_id, tenant_id, action, table_name, row_pk, before, after, ip, ua)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [actor.id||null, actor.tenantId||null, action, table, row_pk, before, after, ip, ua]);
  } catch (e) {
    console.error('[audit] failed', e.message || e);
  }
}
module.exports = { writeAudit };
