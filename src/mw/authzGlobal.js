const { parseActor, hasRole } = require('../utils/auth');

function shouldProtect(pathname) {
  return (
    pathname.startsWith('/api/listings') ||
    pathname.startsWith('/api/purchase-requests') ||
    pathname.startsWith('/api/bids') ||
    pathname.startsWith('/ingest') ||
    pathname.startsWith('/api/bom/plan')
  );
}

function authzGlobal(req, res, next) {
  const actor = parseActor(req);
  res.locals.__actor = actor;

  // Always allow safe reads
  if (req.method === 'GET' || req.method === 'HEAD' || req.method === 'OPTIONS') {
    return next();
  }
  const path = req.path || req.originalUrl || '';
  if (!shouldProtect(path)) return next();

  // Role requirements by endpoint
  const deny = (msg='forbidden') => res.status(403).json({ error: msg, who: actor });

  // Listings
  if (path.startsWith('/api/listings')) {
    if (req.method === 'POST') {
      if (!(hasRole(actor, 'seller', 'admin'))) return deny('seller role required');
      if (req.is('application/json') && typeof req.body === 'object') {
        req.body.tenant_id = req.body.tenant_id || actor.tenantId || null;
        req.body.owner_id = req.body.owner_id || actor.id || null;
        req.body.created_by = req.body.created_by || actor.id || null;
        req.body.updated_by = req.body.updated_by || actor.id || null;
      }
    } else if (req.method === 'DELETE' || req.method === 'PATCH' || req.method === 'PUT') {
      if (!(hasRole(actor, 'admin'))) return deny('admin required for mutations');
    }
    return next();
  }

  // Purchase Requests
  if (path.startsWith('/api/purchase-requests')) {
    if (req.method === 'POST') {
      if (!(hasRole(actor, 'buyer', 'admin'))) return deny('buyer role required');
      if (req.is('application/json') && typeof req.body === 'object') {
        req.body.tenant_id = req.body.tenant_id || actor.tenantId || null;
        req.body.owner_id = req.body.owner_id || actor.id || null;
        req.body.created_by = req.body.created_by || actor.id || null;
        req.body.updated_by = req.body.updated_by || actor.id || null;
      }
    } else if (req.method === 'POST' && /\/confirm$/.test(path)) {
      if (!(hasRole(actor, 'buyer', 'admin'))) return deny('buyer role required');
    }
    return next();
  }

  // Bids
  if (path.startsWith('/api/bids')) {
    if (req.method === 'POST') {
      if (!(hasRole(actor, 'seller', 'admin'))) return deny('seller role required');
      if (req.is('application/json') && typeof req.body === 'object') {
        req.body.tenant_id = req.body.tenant_id || actor.tenantId || null;
        req.body.owner_id = req.body.owner_id || actor.id || null;
        req.body.created_by = req.body.created_by || actor.id || null;
        req.body.updated_by = req.body.updated_by || actor.id || null;
      }
    }
    return next();
  }

  // Ingest (manual/bulk/auto) — admin only
  if (path.startsWith('/ingest')) {
    if (!(hasRole(actor, 'admin'))) return deny('admin required');
    if (req.is('application/json') && typeof req.body === 'object') {
      req.body.tenant_id = req.body.tenant_id || actor.tenantId || null;
      req.body.owner_id = req.body.owner_id || actor.id || null;
      req.body.created_by = req.body.created_by || actor.id || null;
      req.body.updated_by = req.body.updated_by || actor.id || null;
    }
    return next();
  }

  // BOM plan (execute) — buyer/admin
  if (path.startsWith('/api/bom/plan')) {
    if (!(hasRole(actor, 'buyer', 'admin'))) return deny('buyer role required');
    return next();
  }

  next();
}

module.exports = authzGlobal;
