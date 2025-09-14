-- Step 28 — Stripe 결제 연동 + 재고 차감

-- payments 확장
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='payments' AND column_name='provider_session_id') THEN
    ALTER TABLE public.payments ADD COLUMN provider_session_id text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='payments' AND column_name='provider_payment_intent_id') THEN
    ALTER TABLE public.payments ADD COLUMN provider_payment_intent_id text;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='payments' AND column_name='events') THEN
    ALTER TABLE public.payments ADD COLUMN events jsonb;
  END IF;
END $$;

-- listings 수량 음수 방지(권장)
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='chk_listings_qty_nonneg') THEN
    ALTER TABLE public.listings ADD CONSTRAINT chk_listings_qty_nonneg CHECK (quantity_available >= 0);
  END IF;
END $$;
