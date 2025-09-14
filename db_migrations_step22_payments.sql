-- Step 22 — Payments/Settlement Stub: Orders & Invoices & Payments

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- orders
CREATE TABLE IF NOT EXISTS public.orders (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_no text UNIQUE,
  tenant_id text,
  buyer_id text,
  currency text DEFAULT 'USD',
  status text CHECK (status IN ('pending','awaiting_payment','paid','failed','cancelled','fulfilled')) DEFAULT 'pending',
  subtotal_cents bigint DEFAULT 0,
  tax_cents bigint DEFAULT 0,
  shipping_cents bigint DEFAULT 0,
  total_cents bigint DEFAULT 0,
  notes text,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_orders_buyer ON public.orders(buyer_id);
CREATE INDEX IF NOT EXISTS ix_orders_tenant ON public.orders(tenant_id);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_orders_updated_at') THEN
    CREATE TRIGGER trg_orders_updated_at BEFORE UPDATE ON public.orders FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
  END IF;
END $$;

-- order_items
CREATE TABLE IF NOT EXISTS public.order_items (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id uuid REFERENCES public.orders(id) ON DELETE CASCADE,
  brand text, code text, brand_norm text, code_norm text,
  qty integer NOT NULL CHECK (qty > 0),
  unit_price_cents bigint DEFAULT 0,
  currency text DEFAULT 'USD',
  listing_id uuid,
  is_alternative boolean DEFAULT false,
  lead_time_days integer,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_order_items_order ON public.order_items(order_id);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_order_items_updated_at') THEN
    CREATE TRIGGER trg_order_items_updated_at BEFORE UPDATE ON public.order_items FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
  END IF;
END $$;

-- invoices
CREATE TABLE IF NOT EXISTS public.invoices (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  order_id uuid REFERENCES public.orders(id) ON DELETE CASCADE,
  invoice_no text UNIQUE,
  status text CHECK (status IN ('unpaid','paid','void')) DEFAULT 'unpaid',
  currency text DEFAULT 'USD',
  amount_cents bigint NOT NULL,
  issued_at timestamptz DEFAULT now(),
  due_date timestamptz,
  paid_at timestamptz,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_invoices_order ON public.invoices(order_id);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_invoices_updated_at') THEN
    CREATE TRIGGER trg_invoices_updated_at BEFORE UPDATE ON public.invoices FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
  END IF;
END $$;

-- payments (session + record)
CREATE TABLE IF NOT EXISTS public.payments (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  invoice_id uuid REFERENCES public.invoices(id) ON DELETE CASCADE,
  provider text,                 -- e.g., 'fakepg', 'stripe', 'toss'
  provider_session_id text,      -- checkout/session id
  status text CHECK (status IN ('requires_action','authorized','captured','failed','refunded')) DEFAULT 'requires_action',
  amount_cents bigint NOT NULL,
  currency text DEFAULT 'USD',
  raw jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_payments_invoice ON public.payments(invoice_id);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname='trg_payments_updated_at') THEN
    CREATE TRIGGER trg_payments_updated_at BEFORE UPDATE ON public.payments FOR EACH ROW EXECUTE PROCEDURE set_updated_at();
  END IF;
END $$;

-- order_no / invoice_no sequences (simple)
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='seq_order_no') THEN
    CREATE SEQUENCE seq_order_no START 10001 INCREMENT BY 1;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='seq_invoice_no') THEN
    CREATE SEQUENCE seq_invoice_no START 50001 INCREMENT BY 1;
  END IF;
END $$;
