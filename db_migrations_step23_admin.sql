-- Step 23 — Admin & Audit Console
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- listings: moderation fields
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='listings' AND column_name='status') THEN
    ALTER TABLE public.listings ADD COLUMN status text CHECK (status IN ('draft','pending','approved','blocked','archived')) DEFAULT 'approved';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='listings' AND column_name='blocked_reason') THEN
    ALTER TABLE public.listings ADD COLUMN blocked_reason text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='listings' AND column_name='moderated_by') THEN
    ALTER TABLE public.listings ADD COLUMN moderated_by text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='listings' AND column_name='moderated_at') THEN
    ALTER TABLE public.listings ADD COLUMN moderated_at timestamptz;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS ix_listings_status ON public.listings(status);

-- bids: moderation fields
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='bids' AND column_name='status') THEN
    ALTER TABLE public.bids ADD COLUMN status text CHECK (status IN ('active','withdrawn','awarded','blocked')) DEFAULT 'active';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='bids' AND column_name='blocked_reason') THEN
    ALTER TABLE public.bids ADD COLUMN blocked_reason text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='bids' AND column_name='moderated_by') THEN
    ALTER TABLE public.bids ADD COLUMN moderated_by text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='bids' AND column_name='moderated_at') THEN
    ALTER TABLE public.bids ADD COLUMN moderated_at timestamptz;
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS ix_bids_status ON public.bids(status);

-- audit_logs table
CREATE TABLE IF NOT EXISTS public.audit_logs (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  actor_id text,
  tenant_id text,
  action text,                 -- e.g., 'approve.listing', 'block.listing', 'update', 'delete'
  table_name text,
  row_pk text,
  before jsonb,
  after jsonb,
  changed_fields text[],
  ip text,
  ua text,
  created_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_audit_created ON public.audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS ix_audit_table ON public.audit_logs(table_name);
CREATE INDEX IF NOT EXISTS ix_audit_actor ON public.audit_logs(actor_id);

-- generic row change trigger (optional actor from app setting)
DO $$ BEGIN
  CREATE OR REPLACE FUNCTION public.audit_row_change() RETURNS trigger AS $$
  DECLARE
    v_actor text := current_setting('app.actor_id', true);
    v_tenant text := current_setting('app.tenant_id', true);
    before_row jsonb;
    after_row jsonb;
    changed text[];
  BEGIN
    IF (TG_OP = 'INSERT') THEN
      before_row := NULL;
      after_row := to_jsonb(NEW);
    ELSIF (TG_OP = 'UPDATE') THEN
      before_row := to_jsonb(OLD);
      after_row := to_jsonb(NEW);
    ELSIF (TG_OP = 'DELETE') THEN
      before_row := to_jsonb(OLD);
      after_row := NULL;
    END IF;
    -- compute changed fields (rough)
    IF before_row IS NOT NULL AND after_row IS NOT NULL THEN
      SELECT array_agg(k) INTO changed
      FROM (
        SELECT jsonb_object_keys(before_row) AS k
      ) b
      WHERE (before_row->>k) IS DISTINCT FROM (after_row->>k);
    ELSE
      changed := NULL;
    END IF;
    INSERT INTO public.audit_logs(actor_id, tenant_id, action, table_name, row_pk, before, after, changed_fields)
    VALUES (v_actor, v_tenant, TG_OP, TG_TABLE_NAME, COALESCE(NEW.id::text, OLD.id::text), before_row, after_row, changed);
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    ELSE
      RETURN NEW;
    END IF;
  END;
  $$ LANGUAGE plpgsql;
END $$;

-- Attach triggers to selected tables (best-effort)
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['listings','purchase_requests','bids','orders','order_items','invoices','payments','relay_specs']
  LOOP
    EXECUTE format('DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname=%L) THEN
         CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON public.%I
         FOR EACH ROW EXECUTE PROCEDURE public.audit_row_change();
       END IF;
     END $$;', 'trg_audit_'||t, 'trg_audit_'||t, t);
  END LOOP;
END $$;
