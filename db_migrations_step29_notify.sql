-- Step 29 — 판매자 알림 & 입찰 자동화

CREATE TABLE IF NOT EXISTS public.subscriptions (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  actor_id text,
  tenant_id text,
  family_slug text,
  target_email text,
  target_webhook text,
  active boolean DEFAULT true,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_subs_tenant_family ON public.subscriptions(tenant_id, family_slug);
CREATE INDEX IF NOT EXISTS ix_subs_actor ON public.subscriptions(actor_id);

CREATE TABLE IF NOT EXISTS public.notifications (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  type text,
  payload jsonb,
  target text,
  channel text, -- email|webhook
  status text CHECK (status IN ('queued','sent','error')) DEFAULT 'queued',
  error text,
  created_at timestamptz DEFAULT now()
);

-- purchase_requests/bids에 필요한 컬럼 보강(존재 시 스킵)
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='purchase_requests' AND column_name='family_slug') THEN
    ALTER TABLE public.purchase_requests ADD COLUMN family_slug text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='purchase_requests' AND column_name='deadline') THEN
    ALTER TABLE public.purchase_requests ADD COLUMN deadline timestamptz;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='purchase_requests' AND column_name='awarded_qty') THEN
    ALTER TABLE public.purchase_requests ADD COLUMN awarded_qty int DEFAULT 0;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='bids' AND column_name='score') THEN
    ALTER TABLE public.bids ADD COLUMN score numeric;
  END IF;
END $$;
