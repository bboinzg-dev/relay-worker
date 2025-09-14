-- Step 19 — Data Quality & Schema Manager

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- blueprint table additions
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='component_spec_blueprint' AND column_name='validators_json') THEN
    ALTER TABLE public.component_spec_blueprint ADD COLUMN validators_json jsonb;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='component_spec_blueprint' AND column_name='required_fields') THEN
    ALTER TABLE public.component_spec_blueprint ADD COLUMN required_fields text[];
  END IF;
END $$;

-- quality issues table
CREATE TABLE IF NOT EXISTS public.quality_issues (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  family_slug text,
  specs_table text,
  brand_norm text,
  code_norm text,
  field_name text,
  issue_code text,
  severity text CHECK (severity IN ('error','warn','info')),
  message text,
  observed_value text,
  expected jsonb,
  row_pk text,
  meta jsonb,
  created_at timestamptz DEFAULT now(),
  resolved_at timestamptz,
  resolved_by text
);
CREATE INDEX IF NOT EXISTS ix_quality_issues_family ON public.quality_issues(family_slug);
CREATE INDEX IF NOT EXISTS ix_quality_issues_brand_code ON public.quality_issues(brand_norm, code_norm);
CREATE INDEX IF NOT EXISTS ix_quality_issues_status ON public.quality_issues(resolved_at);

-- schema history table
CREATE TABLE IF NOT EXISTS public.schema_history (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  family_slug text,
  specs_table text,
  action text,
  statement jsonb,
  applied_by text,
  applied_at timestamptz DEFAULT now()
);

-- normalize core columns for all spec tables (if registry exists)
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT specs_table FROM public.component_registry LOOP
    EXECUTE format('ALTER TABLE IF EXISTS public.%I
      ADD COLUMN IF NOT EXISTS brand_norm text,
      ADD COLUMN IF NOT EXISTS code_norm text;', r.specs_table);
    EXECUTE format('UPDATE public.%I SET brand_norm=lower(brand) WHERE brand_norm IS NULL AND brand IS NOT NULL;', r.specs_table);
    EXECUTE format('UPDATE public.%I SET code_norm=lower(code) WHERE code_norm IS NULL AND code IS NOT NULL;', r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_brand_norm ON public.%I(brand_norm);', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_code_norm ON public.%I(code_norm);', r.specs_table, r.specs_table);
  END LOOP;
END $$;
