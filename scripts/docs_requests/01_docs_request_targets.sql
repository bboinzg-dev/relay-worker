CREATE TABLE IF NOT EXISTS public.docs_request_targets (
  id uuid PRIMARY KEY DEFAULT public.uuid_generate_v4(),
  docs_request_id uuid NOT NULL REFERENCES public.docs_requests(id) ON DELETE CASCADE,
  target_type text NOT NULL CHECK (target_type IN ('seller','listing','plan_bid','brand_code')),
  seller_id text,
  listing_id uuid,
  plan_bid_id uuid,
  status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','promised','responded','cancelled')),
  promise_date date,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_docs_target_seller ON public.docs_request_targets(seller_id) WHERE seller_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_docs_target_listing ON public.docs_request_targets(listing_id) WHERE listing_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_docs_target_status ON public.docs_request_targets(status);
