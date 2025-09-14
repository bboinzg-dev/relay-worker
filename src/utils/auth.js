function pick(h, k) { return h[k] || h[k.toLowerCase()] || h[k.toUpperCase()] || undefined; }

function parseActor(req) {
  const h = req.headers || {};
  // Preferred explicit dev headers
  const id = pick(h, 'x-actor-id') || null;
  const tenantId = pick(h, 'x-actor-tenant') || null;
  const rolesRaw = pick(h, 'x-actor-roles') || '';
  const roles = String(rolesRaw || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);

  // Fallbacks: try Cloud Run identity headers (optional)
  const email = pick(h, 'x-goog-authenticated-user-email') || null;
  if (!id && email) {
    // email looks like "accounts.google.com:someone@example.com"
    const at = String(email).split(':').pop();
    return { id: at, tenantId, roles: roles.length ? roles : ['user'] };
  }
  return { id, tenantId, roles: roles.length ? roles : ['user'] };
}

function hasRole(actor, ...need) {
  const set = new Set((actor?.roles || []).map(r => r.toLowerCase()));
  return need.some(r => set.has(r.toLowerCase()));
}

module.exports = { parseActor, hasRole };
