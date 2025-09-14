-- Step 21 — Search UX: indexes & helpers

CREATE EXTENSION IF NOT EXISTS pg_trgm;

DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT specs_table FROM public.component_registry LOOP
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_trgm_brand ON public.%I USING gin (brand gin_trgm_ops);', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_trgm_code  ON public.%I USING gin (code gin_trgm_ops);', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_trgm_disp  ON public.%I USING gin (display_name gin_trgm_ops);', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_series ON public.%I(lower(series));', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_family ON public.%I(lower(family_slug));', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_contact_form ON public.%I(lower(contact_form));', r.specs_table, r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS ix_%I_coil_v ON public.%I(coil_voltage_vdc);', r.specs_table, r.specs_table);
  END LOOP;
END $$;

-- listings helper indexes
CREATE INDEX IF NOT EXISTS ix_listings_brand_code ON public.listings(brand_norm, code_norm);
CREATE INDEX IF NOT EXISTS ix_listings_price ON public.listings(price_cents);
CREATE INDEX IF NOT EXISTS ix_listings_lead ON public.listings(lead_time_days);
