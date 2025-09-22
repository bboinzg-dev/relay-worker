const db = require('./db');
const { v4: uuidv4 } = require('uuid');

let ensured = false;

async function ensureEventQueue() {
  if (ensured) return;
  await db.query(`
    DO $block$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'gen_event_queue_uuid') THEN
        EXECUTE $func$
          CREATE FUNCTION public.gen_event_queue_uuid() RETURNS uuid AS
          $body$
          DECLARE
            result uuid;
            fallback text;
          BEGIN
            BEGIN
              EXECUTE 'SELECT uuid_generate_v4()' INTO result;
              IF result IS NOT NULL THEN
                RETURN result;
              END IF;
            EXCEPTION WHEN undefined_function THEN
            END;
            BEGIN
              EXECUTE 'SELECT gen_random_uuid()' INTO result;
              IF result IS NOT NULL THEN
                RETURN result;
              END IF;
            EXCEPTION WHEN undefined_function THEN
            END;
            fallback := md5(random()::text || clock_timestamp()::text);
            RETURN (
              substr(fallback, 1, 8) || '-' ||
              substr(fallback, 9, 4) || '-' ||
              substr(fallback, 13, 4) || '-' ||
              substr(fallback, 17, 4) || '-' ||
              substr(fallback, 21, 12)
            )::uuid;
          END;
          $body$
          LANGUAGE plpgsql;
        $func$;
      END IF;
    END
    $block$;
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS public.event_queue (
      id uuid PRIMARY KEY DEFAULT public.gen_event_queue_uuid(),
      type text NOT NULL,
      payload jsonb NOT NULL,
      status text CHECK (status IN ('queued','processing','done','error')) DEFAULT 'queued',
      attempts int DEFAULT 0,
      last_error text,
      run_at timestamptz DEFAULT now(),
      created_at timestamptz DEFAULT now(),
      updated_at timestamptz DEFAULT now()
    );
  `);
  await db.query(`CREATE INDEX IF NOT EXISTS ix_event_queue_status ON public.event_queue(status, run_at);`);
  await db.query(`
    DO $block$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'set_updated_at') THEN
        EXECUTE $func$
          CREATE FUNCTION public.set_updated_at() RETURNS trigger AS
          $body$
          BEGIN
            NEW.updated_at = now();
            RETURN NEW;
          END;
          $body$
          LANGUAGE plpgsql;
        $func$;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_event_queue_updated_at') THEN
        EXECUTE 'CREATE TRIGGER trg_event_queue_updated_at BEFORE UPDATE ON public.event_queue FOR EACH ROW EXECUTE PROCEDURE public.set_updated_at();';
      END IF;
    END
    $block$;
  `).catch(() => {});
  ensured = true;
}

async function enqueueEvent(type, payload = {}, { runAt = null, status = null } = {}) {
  if (!type) throw new Error('type required');
  await ensureEventQueue();
  const columns = ['id', 'type', 'payload'];
  const values = [uuidv4(), type, payload ?? {}];
  if (runAt) {
    columns.push('run_at');
    values.push(runAt instanceof Date ? runAt.toISOString() : runAt);
  }
  if (status) {
    columns.push('status');
    values.push(status);
  }
  const placeholders = columns.map((_, idx) => `$${idx + 1}`);
  await db.query(
    `INSERT INTO public.event_queue (${columns.join(', ')}) VALUES (${placeholders.join(', ')})`,
    values
  );
  return values[0];
}

module.exports = { ensureEventQueue, enqueueEvent };