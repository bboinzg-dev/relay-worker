-- Ensure BTREE(lower(...)) and GIN FTS indexes exist for all component specs tables.
DO $$DECLARE r RECORD; BEGIN
  FOR r IN SELECT specs_table FROM public.component_registry LOOP
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON public.%I ((lower(brand)))', r.specs_table || '_bt_lower_brand', r.specs_table);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON public.%I ((lower(pn)))',    r.specs_table || '_bt_lower_pn',    r.specs_table);
  END LOOP;
END $$;

DO $$DECLARE r RECORD; BEGIN
  FOR r IN SELECT specs_table FROM public.component_registry LOOP
    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I ON public.%I USING GIN (to_tsvector(''simple'', '
      || 'coalesce(series,'''')||'' ''||coalesce(contact_form,'''')||'' ''||coalesce(raw_json::text,'''')'
      || '))',
      r.specs_table || '_gin_fts', r.specs_table
    );
  END LOOP;
END $$;