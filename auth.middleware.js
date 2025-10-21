const jwt = require('jsonwebtoken');

function pick(h, k) {
  if (!h) return undefined;
  return h[k] || h[k.toLowerCase()] || h[k.toUpperCase()];
}

function parseAppActor(req) {
  const headers = req?.headers || {};
  const hApp = pick(headers, 'x-app-auth');
  const pickBearer = (hdr) => (hdr && hdr.startsWith('Bearer ')) ? hdr.slice(7) : null;

  const appToken = pickBearer(hApp);
  if (!appToken) {
    return { ok: false, reason: 'no_app_token' };
  }

  const secret = process.env.JWT_SECRET;
  if (!secret) {
    return { ok: false, reason: 'missing_jwt_secret' };
  }

  try {
    const dec = jwt.verify(appToken, secret, { algorithms: ['HS256'] });
    return { ok: true, user: dec };
  } catch (e) {
    return { ok: false, reason: e?.message || 'verify_failed' };
  }
}

function requireSeller(req, res, next) {
  const result = parseAppActor(req);
  if (!result.ok) {
    return res.status(401).json({ ok: false, error: 'unauthorized_app_jwt', reason: result.reason });
  }

  const roles = Array.isArray(result.user?.roles)
    ? result.user.roles.map((r) => String(r).toLowerCase())
    : [];
  if (!roles.includes('seller') && !roles.includes('admin')) {
    return res.status(403).json({ ok: false, error: 'seller_role_required' });
  }

  req.actor = result.user;
  return next();
}

module.exports = { parseAppActor, requireSeller };
