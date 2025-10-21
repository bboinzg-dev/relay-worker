const jwt = require('jsonwebtoken');

function parseAppActor(req) {
  const headers = req?.headers || {};
  const hdr = headers['x-app-auth'] || headers['X-App-Auth'];
  const pickBearer = (value) => (value && value.startsWith('Bearer ')) ? value.slice(7) : null;

  const appToken = pickBearer(hdr);
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
    console.warn('[auth] app_jwt_verify_failed:', e?.message);
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
