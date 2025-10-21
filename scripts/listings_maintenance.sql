-- listings maintenance helpers
-- 판매자 + 품목(brand_norm+code_norm) 중복 등록 방지 (archived는 예외)
CREATE UNIQUE INDEX IF NOT EXISTS ux_listings_seller_brand_code
ON public.listings (seller_id, brand_norm, code_norm)
WHERE status <> 'archived';

-- updated_at 자동 갱신
CREATE OR REPLACE FUNCTION public._touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at := now(); RETURN NEW; END$$;

DROP TRIGGER IF EXISTS trg_listings_touch ON public.listings;
CREATE TRIGGER trg_listings_touch
BEFORE UPDATE ON public.listings
FOR EACH ROW EXECUTE FUNCTION public._touch_updated_at();

-- 조회 보강 인덱스
CREATE INDEX IF NOT EXISTS ix_listings_seller ON public.listings (seller_id);
CREATE INDEX IF NOT EXISTS ix_listings_status_updated ON public.listings (status, updated_at DESC);
