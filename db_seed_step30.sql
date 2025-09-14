-- Step 30 — 시드 데이터 (샘플 부품/재고/주문 흐름)

-- 샘플 부품
INSERT INTO public.relay_specs (brand, code, brand_norm, code_norm, series, family_slug, display_name, coil_voltage_vdc, contact_form, datasheet_url)
VALUES ('omron','G2R-1A', lower('omron'), lower('G2R-1A'), 'G2R', 'relay', 'Omron G2R-1A', 24, '1A', 'gs://partsplan-docai-us/datasheets/g2r-1a.pdf')
ON CONFLICT DO NOTHING;

-- 샘플 재고(두 셀러)
INSERT INTO public.listings (brand, code, brand_norm, code_norm, price_cents, currency, quantity_available, lead_time_days, status, seller_id)
VALUES
  ('omron','G2R-1A', lower('omron'), lower('G2R-1A'), 350, 'USD', 100, 3, 'approved', 'sellerA'),
  ('omron','G2R-1A', lower('omron'), lower('G2R-1A'), 320, 'USD', 50, 7, 'approved', 'sellerB')
ON CONFLICT DO NOTHING;

-- 샘플 RFQ
INSERT INTO public.purchase_requests (id, tenant_id, family_slug, brand, code, requested_qty, deadline, status, buyer_id)
VALUES (gen_random_uuid(), NULL, 'relay', 'omron','G2R-1A', 30, now() + interval '1 day', 'open', 'buyer1')
ON CONFLICT DO NOTHING;
