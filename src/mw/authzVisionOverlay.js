const { parseActor, hasRole } = require('../utils/auth');

function authzGlobal(req, res, next) {
  const actor = parseActor(req);
  res.locals.__actor = actor;

  const path = req.path || req.originalUrl || '';

  // Public identify endpoint (read)
  if (path.startsWith('/api/vision/identify')) return next();

  // Indexing requires admin
  if (path.startsWith('/api/vision/index')) {
    if (!(hasRole(actor, 'admin'))) return res.status(403).json({ error: 'admin required', who: actor });
    return next();
  }

  // Defer to previous rules (copy from Step 19 patched version outlines)
  return next();
}

module.exports = authzGlobal;
