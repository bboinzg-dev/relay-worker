const jwt = require('jsonwebtoken');
const { OAuth2Client } = require('google-auth-library');

const gclient = new OAuth2Client();

const clean = (value) => String(value || '').trim().replace(/^['"]|['"]$/g, '');
const pickBearer = (value) => {
  const token = clean(value);
  if (!token) {
    return null;
  }
  return token.startsWith('Bearer ') ? token.slice(7).trim() : token;
};

function parseAppActor(req) {
  const headers = req?.headers || {};
  const hdr = headers['x-app-auth']
    || headers['X-App-Auth']
    || headers['x_app_auth']
    || headers['x-appauth']
    || headers['x_appauth'];

  const body = (req.body && typeof req.body === 'object') ? req.body : null;
  const query = req.query || null;
  const fromBody = body?._app_auth || body?.app_auth || body?.appAuth;
  const fromQuery = query?.app_auth || query?.appAuth;

  const appToken = pickBearer(hdr)
    || pickBearer(fromBody)
    || pickBearer(fromQuery);
  if (!appToken) {
    console.warn('[auth] missing X-App-Auth header');
    return { ok: false, reason: 'no_app_token' };
  }

  const secret = (process.env.JWT_SECRET || '').trim();
  if (!secret) {
    return { ok: false, reason: 'missing_jwt_secret' };
  }

  try {
    const dec = jwt.verify(appToken, secret, { algorithms: ['HS256'] });
    return { ok: true, user: dec };
  } catch (e) {
    console.warn('[auth] jwt verify failed:', e?.message || e);
    return { ok: false, reason: e?.message || 'verify_failed' };
  }
}

async function parseIdTokenAsSeller(req) {
  const idToken = pickBearer(req.headers?.authorization);
  if (!idToken) {
    return null;
  }

  try {
    const audience = process.env.WORKER_AUDIENCE;
    if (!audience) {
      console.warn('[auth] missing_worker_audience_for_idtoken_fallback');
      return null;
    }
    const ticket = await gclient.verifyIdToken({ idToken, audience });
    const payload = ticket.getPayload() || {};
    return { sub: payload.sub || 'idtoken', roles: ['seller'] };
  } catch (err) {
    console.warn('[auth] idtoken_fallback_failed:', err?.message);
    return null;
  }
}

function hasSellerRole(user) {
  const roles = Array.isArray(user?.roles)
    ? user.roles.map((r) => String(r).toLowerCase())
    : [];
  return roles.includes('seller') || roles.includes('admin');
}

function requireSeller(req, res, next) {
  (async () => {
    const result = parseAppActor(req);
    if (result.ok) {
      if (!hasSellerRole(result.user)) {
        return res.status(403).json({ ok: false, error: 'seller_role_required' });
      }
      req.actor = result.user;
      return next();
    }

    if (process.env.ALLOW_IDTOKEN_AS_APP === '1') {
      const fallbackUser = await parseIdTokenAsSeller(req);
      if (fallbackUser) {
        req.actor = fallbackUser;
        return next();
      }
    }

    return res.status(401).json({ ok: false, error: 'unauthorized_app_jwt', reason: result.reason });
  })().catch(next);
}

module.exports = { parseAppActor, requireSeller };
