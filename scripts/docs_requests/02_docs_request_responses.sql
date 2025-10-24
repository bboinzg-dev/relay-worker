CREATE TABLE IF NOT EXISTS public.docs_request_responses (
  id uuid PRIMARY KEY DEFAULT public.uuid_generate_v4(),
  docs_request_id uuid NOT NULL REFERENCES public.docs_requests(id) ON DELETE CASCADE,
  target_id uuid REFERENCES public.docs_request_targets(id) ON DELETE SET NULL,
  responder_user_id bigint,
  kind text NOT NULL CHECK (kind IN ('upload','promise')),
  note text,
  promised_date date,
  file_blob_id bigint REFERENCES public.file_blobs(id),
  file_url text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW public.vw_docs_requests_for_seller AS
SELECT
  t.id AS target_id,
  t.docs_request_id,
  t.target_type,
  t.seller_id,
  t.listing_id,
  t.status,
  t.promise_date,
  dr.requester_user_id,
  dr.manufacturer AS brand,
  dr.part_number AS code,
  dr.docs,
  dr.status AS request_status,
  dr.created_at AS requested_at,
  l.unit_price_cents,
  l.currency,
  l.qty_available
FROM public.docs_request_targets t
JOIN public.docs_requests dr ON dr.id = t.docs_request_id
LEFT JOIN public.listings l ON l.id = t.listing_id;

CREATE OR REPLACE VIEW public.vw_docs_requests_for_listings AS
SELECT *
FROM public.vw_docs_requests_for_seller
WHERE listing_id IS NOT NULL;
