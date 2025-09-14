-- Step 25 — Quality/Validation pipeline v1

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- quality_rules: per-family rules
CREATE TABLE IF NOT EXISTS public.quality_rules (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  family_slug text UNIQUE,
  rules_json jsonb,
  updated_at timestamptz DEFAULT now()
);

-- quality_runs: scan executions
CREATE TABLE IF NOT EXISTS public.quality_runs (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  family_slug text,
  started_at timestamptz DEFAULT now(),
  finished_at timestamptz,
  status text CHECK (status IN ('running','succeeded','failed')) DEFAULT 'running',
  counts jsonb
);
CREATE INDEX IF NOT EXISTS ix_quality_runs_family ON public.quality_runs(family_slug);

-- quality_issues enrichment (Step19에서 생성됨을 전제, 없으면 생성)
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='quality_issues') THEN
    CREATE TABLE public.quality_issues (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      family_slug text,
      table_name text,
      row_ref text,
      field text,
      type text,        -- missing, duplicate, outlier, invalid, normalization, link_missing, etc
      severity text,    -- error, warn, info
      message text,
      suggestion_json jsonb,
      run_id uuid REFERENCES public.quality_runs(id) ON DELETE SET NULL,
      status text CHECK (status IN ('open','accepted','fixed','wontfix')) DEFAULT 'open',
      created_at timestamptz DEFAULT now(),
      resolved_at timestamptz,
      accepted_by text,
      fixed_by text
    );
    CREATE INDEX ix_quality_issues_family ON public.quality_issues(family_slug);
    CREATE INDEX ix_quality_issues_status ON public.quality_issues(status);
    CREATE INDEX ix_quality_issues_type ON public.quality_issues(type);
  END IF;
END $$;

-- Add new columns if missing
DO $$ BEGIN
  PERFORM 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='quality_issues' AND column_name='suggestion_json';
  IF NOT FOUND THEN
    ALTER TABLE public.quality_issues ADD COLUMN suggestion_json jsonb;
  END IF;
  PERFORM 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='quality_issues' AND column_name='accepted_by';
  IF NOT FOUND THEN
    ALTER TABLE public.quality_issues ADD COLUMN accepted_by text;
  END IF;
  PERFORM 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='quality_issues' AND column_name='fixed_by';
  IF NOT FOUND THEN
    ALTER TABLE public.quality_issues ADD COLUMN fixed_by text;
  END IF;
  PERFORM 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='quality_issues' AND column_name='run_id';
  IF NOT FOUND THEN
    ALTER TABLE public.quality_issues ADD COLUMN run_id uuid REFERENCES public.quality_runs(id) ON DELETE SET NULL;
  END IF;
END $$;
