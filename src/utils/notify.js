const db = require('../../db');
const { enqueueNotify } = require('./tasks');

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

async function ensureNotificationJobs() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.notification_jobs (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      event_id uuid NOT NULL,
      target_account_id uuid,
      webhook_url text NOT NULL,
      payload jsonb NOT NULL,
      status text NOT NULL DEFAULT 'queued', -- queued|delivered|failed
      attempt_count integer NOT NULL DEFAULT 0,
      last_error text,
      delivered_at timestamptz,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (event_id, target_account_id)
    );
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS ix_notification_jobs_status ON public.notification_jobs(status, created_at);`);
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

async function createNotificationJobs({ event_id, recipients=[], payload }) {
  await ensureNotificationJobs();
  const jobs = [];
  for (const r of recipients) {
    if (!r.webhook_url) continue; // skip if no webhook
    try {
      const row = await db.query(
        `INSERT INTO public.notification_jobs (event_id, target_account_id, webhook_url, payload)
         VALUES ($1,$2,$3,$4)
         ON CONFLICT (event_id, target_account_id) DO UPDATE
           SET webhook_url=EXCLUDED.webhook_url, payload=EXCLUDED.payload, status='queued', updated_at=now()
         RETURNING *`,
        [event_id, r.id, r.webhook_url, payload]
      );
      jobs.push(row.rows[0]);
    } catch (e) {
      // if unique conflict without RETURNING (older PG), try selecting
      const q = await db.query(`SELECT * FROM public.notification_jobs WHERE event_id=$1 AND target_account_id=$2`, [event_id, r.id]);
      if (q.rows.length) jobs.push(q.rows[0]);
    }
  }
  return jobs;
}

async function enqueueJobs(jobs) {
  const out = [];
  for (const j of jobs) {
    try {
      const name = await enqueueNotify(j.id);
      out.push({ id: j.id, task: name });
    } catch (e) {
      out.push({ id: j.id, error: String(e.message || e) });
    }
  }
  return out;
}

// Backwards-compatible notify() facade — now async via Cloud Tasks.
async function notify(type, { tenant_id=null, actor_id=null, family_slug=null, brand=null, code=null, data={} }) {
  const recips = await findSellerRecipients({ family_slug, brand, code });
  const evt = await recordEvent({ type, tenant_id, actor_id, targets: recips.map(r => r.id), payload: { family_slug, brand, code, data } });
  const jobs = await createNotificationJobs({ event_id: evt.id, recipients: recips, payload: { event_id: evt.id, type, family_slug, brand, code, data } });
  const enq = await enqueueJobs(jobs);
  return { event: evt, jobs: enq };
}

// for tasks handler
async function markJob(id, patch) {
  const sets = [];
  const params = [];
  let i = 1;
  for (const [k, v] of Object.entries(patch)) {
    sets.push(`${k} = $${i++}`);
    params.push(v);
  }
  params.push(id);
  const sql = `UPDATE public.notification_jobs SET ${sets.join(', ')}, updated_at = now() WHERE id = $${i} RETURNING *`;
  const r = await db.query(sql, params);
  return r.rows[0];
}

module.exports = { notify, recordEvent, findFamilyForBrandCode, ensureNotificationJobs, markJob };
