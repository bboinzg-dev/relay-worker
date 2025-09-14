-- Step 18 — Authorization/Tenancy columns & triggers

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- helper trigger to auto-update updated_at
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'set_updated_at') THEN
    CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
    BEGIN NEW.updated_at = now(); RETURN NEW; END;
    $$ LANGUAGE plpgsql;
  END IF;
END $$;

-- target tables
DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['listings','purchase_requests','bids','relay_specs']
  LOOP
    EXECUTE format('ALTER TABLE IF EXISTS public.%I
      ADD COLUMN IF NOT EXISTS tenant_id text,
      ADD COLUMN IF NOT EXISTS owner_id text,
      ADD COLUMN IF NOT EXISTS created_by text,
      ADD COLUMN IF NOT EXISTS updated_by text,
      ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();', t);

    -- indexes
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_tenant ON public.%I(tenant_id);', t, t);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_owner  ON public.%I(owner_id);', t, t);

    -- trigger
    EXECUTE format('DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = %L) THEN CREATE TRIGGER %I BEFORE UPDATE ON public.%I FOR EACH ROW EXECUTE PROCEDURE set_updated_at(); END IF; END $$;',
      'trg_'||t||'_updated_at', 'trg_'||t||'_updated_at', t);
  END LOOP;
END $$;

-- propagate to all registered spec tables (if registry exists)
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT specs_table FROM public.component_registry LOOP
    EXECUTE format('ALTER TABLE IF EXISTS public.%I
      ADD COLUMN IF NOT EXISTS tenant_id text,
      ADD COLUMN IF NOT EXISTS owner_id text,
      ADD COLUMN IF NOT EXISTS created_by text,
      ADD COLUMN IF NOT EXISTS updated_by text,
      ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now(),
      ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();', r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_tenant ON public.%I(tenant_id);', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_owner  ON public.%I(owner_id);', r.specs_table, r.specs_table);
    EXECUTE format('DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = %L) THEN CREATE TRIGGER %I BEFORE UPDATE ON public.%I FOR EACH ROW EXECUTE PROCEDURE set_updated_at(); END IF; END $$;',
      'trg_'||r.specs_table||'_updated_at', 'trg_'||r.specs_table||'_updated_at', r.specs_table);
  END LOOP;
END $$;
