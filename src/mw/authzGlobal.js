const { parseActor, hasRole } = require('../utils/auth');

function shouldProtect(pathname) {
  return (
    pathname.startsWith('/api/listings') ||
    pathname.startsWith('/api/purchase-requests') ||
    pathname.startsWith('/api/bids') ||
    pathname.startsWith('/ingest') ||
    pathname.startsWith('/api/bom/plan') ||
    pathname.startsWith('/api/schema') ||
    pathname.startsWith('/api/quality')
  );
}

function authzGlobal(req, res, next) {
  const actor = parseActor(req);
  res.locals.__actor = actor;

  const path = req.path || req.originalUrl || '';

  // allow safe reads for most, but schema/quality are admin-only (read & write)
  if ((req.method === 'GET' || req.method === 'HEAD' || req.method === 'OPTIONS')) {
    if (path.startsWith('/api/schema') || path.startsWith('/api/quality')) {
      if (!(hasRole(actor, 'admin'))) return res.status(403).json({ error: 'admin required', who: actor });
    }
    return next();
  }

  if (!shouldProtect(path)) return next();

  const deny = (msg='forbidden') => res.status(403).json({ error: msg, who: actor });

  if (path.startsWith('/api/listings')) {
    if (req.method === 'POST') {
      if (!(hasRole(actor, 'seller', 'admin'))) return deny('seller role required');
    } else if (req.method === 'DELETE' || req.method === 'PATCH' || req.method === 'PUT') {
      if (!(hasRole(actor, 'admin'))) return deny('admin required for mutations');
    }
    return next();
  }

  if (path.startsWith('/api/purchase-requests')) {
    if (!(hasRole(actor, 'buyer', 'admin'))) return deny('buyer role required');
    return next();
  }

  if (path.startsWith('/api/bids')) {
    if (!(hasRole(actor, 'seller', 'admin'))) return deny('seller role required');
    return next();
  }

  if (path.startsWith('/ingest')) {
    if (!(hasRole(actor, 'admin'))) return deny('admin required');
    return next();
  }

  if (path.startsWith('/api/bom/plan')) {
    if (!(hasRole(actor, 'buyer', 'admin'))) return deny('buyer role required');
    return next();
  }

  if (path.startsWith('/api/schema') || path.startsWith('/api/quality')) {
    if (!(hasRole(actor, 'admin'))) return deny('admin required');
    return next();
  }

  next();
}

module.exports = authzGlobal;
