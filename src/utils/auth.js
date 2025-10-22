function pick(h, k) { return h?.[k] || h?.[k.toLowerCase()] || h?.[k.toUpperCase()] || undefined; }

function parseActor(req) {
  const h = req.headers || {};
  const id = pick(h, 'x-actor-id') || null;
  const tenantId = pick(h, 'x-actor-tenant') || null;
  const rolesRaw = pick(h, 'x-actor-roles') || '';
  const roles = String(rolesRaw || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
  return {
    id: id ? String(id) : null,
    tenantId: tenantId != null ? String(tenantId) : null,
    roles: roles.length ? roles : ['user']
  };
}

function hasRole(actor, ...need) {
  const set = new Set((actor?.roles || []).map(r => r.toLowerCase()));
  return need.some(r => set.has(r.toLowerCase()));
}

module.exports = { parseActor, hasRole };
