-- Step 24 — Tenancy + Actor Propagation + Access Control

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Helper function: set app.* variables in-session (transaction-scoped if SET LOCAL used)
CREATE OR REPLACE FUNCTION public.fn_set_actor(actor_id text, tenant_id text, roles text[])
RETURNS void AS $$
BEGIN
  PERFORM set_config('app.actor_id', COALESCE(actor_id,''), true);
  PERFORM set_config('app.tenant_id', COALESCE(tenant_id,''), true);
  PERFORM set_config('app.roles', array_to_string(COALESCE(roles, ARRAY[]::text[]), ','), true);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Add tenant_id to key tables (if missing)
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['listings','purchase_requests','bids','orders','order_items','invoices','payments','image_index','quality_issues']
  LOOP
    EXECUTE format('DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=''public'' AND table_name=%L AND column_name=''tenant_id'') THEN
        ALTER TABLE public.%I ADD COLUMN tenant_id text;
      END IF;
    END $$;', t, t);
  END LOOP;
END $$;

-- Backfill tenant_id from known parents where possible
DO $$
BEGIN
  -- order_items from orders
  UPDATE public.order_items oi SET tenant_id = o.tenant_id
    FROM public.orders o WHERE oi.order_id=o.id AND (oi.tenant_id IS DISTINCT FROM o.tenant_id);
  -- invoices from orders
  UPDATE public.invoices i SET tenant_id = o.tenant_id
    FROM public.orders o WHERE i.order_id=o.id AND (i.tenant_id IS DISTINCT FROM o.tenant_id);
  -- payments from invoices
  UPDATE public.payments p SET tenant_id = i.tenant_id
    FROM public.invoices i WHERE p.invoice_id=i.id AND (p.tenant_id IS DISTINCT FROM i.tenant_id);
END $$;

-- Helpful indexes for tenancy scopes
CREATE INDEX IF NOT EXISTS ix_listings_tenant_brand_code ON public.listings(tenant_id, brand_norm, code_norm);
CREATE INDEX IF NOT EXISTS ix_bids_tenant ON public.bids(tenant_id);
CREATE INDEX IF NOT EXISTS ix_purchase_requests_tenant ON public.purchase_requests(tenant_id);
CREATE INDEX IF NOT EXISTS ix_orders_tenant ON public.orders(tenant_id);
CREATE INDEX IF NOT EXISTS ix_order_items_tenant ON public.order_items(tenant_id);
CREATE INDEX IF NOT EXISTS ix_invoices_tenant ON public.invoices(tenant_id);
CREATE INDEX IF NOT EXISTS ix_payments_tenant ON public.payments(tenant_id);
CREATE INDEX IF NOT EXISTS ix_image_index_tenant ON public.image_index(tenant_id);
CREATE INDEX IF NOT EXISTS ix_quality_issues_tenant ON public.quality_issues(tenant_id);

-- Convenience view: approved listings visible for a tenant (NULL is global)
CREATE OR REPLACE VIEW public.v_listings_effective AS
SELECT *
FROM public.listings
WHERE status='approved'
  AND (tenant_id IS NULL OR tenant_id = NULLIF(current_setting('app.tenant_id', true), ''));

