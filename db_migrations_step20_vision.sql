-- Step 20 — Vision v1 (image index)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='image_index') THEN
    CREATE TABLE public.image_index (
      id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
      family_slug text,
      brand_norm text,
      code_norm text,
      gcs_uri text,
      image_sha256 text UNIQUE,
      embedding vector(1408),
      meta jsonb,
      created_at timestamptz DEFAULT now()
    );
    CREATE INDEX ix_image_index_brand_code ON public.image_index(brand_norm, code_norm);
    -- ivfflat index is optional; require pgvector >= 0.5
    CREATE INDEX ix_image_index_vec ON public.image_index USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
  END IF;
END $$;
