-- Daily FX snapshot support
CREATE TABLE IF NOT EXISTS public.fx_rates_daily (
  provider     text NOT NULL DEFAULT 'koreaexim',
  currency     text NOT NULL,
  rate_date    date NOT NULL,
  rate         numeric(18,6) NOT NULL,
  source       text NOT NULL DEFAULT 'exim_deal_bas_daily',
  collected_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (currency, rate_date)
);

ALTER TABLE public.fx_rates_daily
  ALTER COLUMN provider SET DEFAULT 'koreaexim',
  ALTER COLUMN source SET DEFAULT 'exim_deal_bas_daily';

ALTER TABLE public.listings
  ADD COLUMN IF NOT EXISTS unit_price_krw_cents integer,
  ADD COLUMN IF NOT EXISTS unit_price_fx_rate numeric(18,6),
  ADD COLUMN IF NOT EXISTS unit_price_fx_yyyymm integer,
  ADD COLUMN IF NOT EXISTS unit_price_fx_src text;

ALTER TABLE public.bids
  ADD COLUMN IF NOT EXISTS unit_price_krw_cents integer,
  ADD COLUMN IF NOT EXISTS unit_price_fx_rate numeric(18,6),
  ADD COLUMN IF NOT EXISTS unit_price_fx_yyyymm integer,
  ADD COLUMN IF NOT EXISTS unit_price_fx_src text;

