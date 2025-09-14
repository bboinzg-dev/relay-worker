-- Step 26 — Ops automation v2: ingest hooks + thumb regen + signed URL cache

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- helper: set_updated_at (idempotent)
DO $$ BEGIN
  CREATE OR REPLACE FUNCTION public.set_updated_at() RETURNS trigger AS $$
  BEGIN NEW.updated_at = now(); RETURN NEW; END;
  $$ LANGUAGE plpgsql;
END $$;

-- event queue
CREATE TABLE IF NOT EXISTS public.event_queue (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  type text NOT NULL,
  payload jsonb NOT NULL,
  status text CHECK (status IN ('queued','processing','done','error')) DEFAULT 'queued',
  attempts int DEFAULT 0,
  last_error text,
  run_at timestamptz DEFAULT now(),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_event_queue_status ON public.event_queue(status, run_at);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_event_queue_updated_at') THEN
    CREATE TRIGGER trg_event_queue_updated_at BEFORE UPDATE ON public.event_queue FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
  END IF;
END $$;

-- signed URL cache
CREATE TABLE IF NOT EXISTS public.signed_url_cache (
  gcs_uri text PRIMARY KEY,
  url text NOT NULL,
  content_type text,
  expires_at timestamptz NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_signed_url_cache_updated_at') THEN
    CREATE TRIGGER trg_signed_url_cache_updated_at BEFORE UPDATE ON public.signed_url_cache FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
  END IF;
END $$;

-- optional trigger: enqueue spec-upsert (relay_specs only as example)
DO $$ BEGIN
  CREATE OR REPLACE FUNCTION public.fn_enqueue_spec_event() RETURNS trigger AS $$
  DECLARE fam text;
  BEGIN
    -- family_slug 컬럼이 없을 수도 있으니 best-effort
    BEGIN
      EXECUTE format('SELECT $1.%I', 'family_slug') USING NEW INTO fam;
    EXCEPTION WHEN undefined_column THEN
      fam := NULL;
    END;
    INSERT INTO public.event_queue (type, payload)
    VALUES ('spec_upsert', jsonb_build_object('family_slug', fam, 'brand', NEW.brand, 'code', NEW.code,
                                              'datasheet_url', NEW.datasheet_url, 'cover', NEW.cover));
    RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_enqueue_spec_relay') THEN
    CREATE TRIGGER trg_enqueue_spec_relay AFTER INSERT OR UPDATE ON public.relay_specs
    FOR EACH ROW EXECUTE PROCEDURE public.fn_enqueue_spec_event();
  END IF;
END $$;
