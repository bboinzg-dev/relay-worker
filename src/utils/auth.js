function pick(h, k) { return h?.[k] || h?.[k.toLowerCase()] || h?.[k.toUpperCase()] || undefined; }

function parseActor(req) {
  if (req?.actor && typeof req.actor === 'object') {
    const actor = req.actor;
    const actorId = actor.id ?? actor.sub ?? null;
    const tenantId = actor.tenantId ?? actor.tenant_id ?? null;
    const rolesRaw = Array.isArray(actor.roles)
      ? actor.roles
      : String(actor.roles || '').split(',');
    const roles = rolesRaw
      .map((r) => String(r || '').trim().toLowerCase())
      .filter(Boolean);
    const username = actor.username ?? actor.user_name ?? actor.name ?? null;
    const userId = actor.user_id ?? actor.userId ?? actorId;
    return {
      id: actorId != null ? String(actorId) : null,
      user_id: userId != null ? String(userId) : null,
      tenantId: tenantId != null ? String(tenantId) : null,
      username: username != null ? String(username) : null,
      roles: roles.length ? roles : ['user'],
    };
  }

  const h = req?.headers || {};
  const id = pick(h, 'x-actor-id') || pick(h, 'x-user-id') || null;
  const tenantId = pick(h, 'x-actor-tenant') || null;
  const rolesRaw = pick(h, 'x-actor-roles') || '';
  const username = pick(h, 'x-actor-username') || pick(h, 'x-user-username') || null;
  const roles = String(rolesRaw || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
  return {
    id: id ? String(id) : null,
    user_id: id ? String(id) : null,
    tenantId: tenantId != null ? String(tenantId) : null,
    username: username != null ? String(username) : null,
    roles: roles.length ? roles : ['user']
  };
}

function toTrimmedSet(values) {
  const seen = new Set();
  for (const value of values) {
    if (value == null) continue;
    const text = String(value).trim();
    if (!text) continue;
    if (!seen.has(text)) {
      seen.add(text);
    }
  }
  return Array.from(seen);
}

function getSellerKeySet(req) {
  let actor = null;
  try {
    actor = parseActor(req) || null;
  } catch {
    actor = null;
  }

  const rawActor = req?.actor && typeof req.actor === 'object' ? req.actor : null;
  const user = req?.user && typeof req.user === 'object' ? req.user : null;
  const headers = req?.headers || {};
  const query = req?.query || {};

  const idCandidates = [
    actor?.user_id,
    actor?.id,
    rawActor?.user_id,
    rawActor?.id,
    rawActor?.sub,
    user?.id,
    headers['x-user-id'],
    headers['x-actor-id'],
    headers['x-user'],
    query.user_id,
    query.seller_id,
  ];

  const usernameCandidates = [
    actor?.username,
    rawActor?.username,
    user?.username,
    headers['x-actor-username'],
    headers['x-user-username'],
    headers['x-user-name'],
  ];

  return toTrimmedSet([...idCandidates, ...usernameCandidates]);
}

function hasRole(actor, ...need) {
  const set = new Set((actor?.roles || []).map(r => r.toLowerCase()));
  return need.some(r => set.has(r.toLowerCase()));
}

module.exports = { parseActor, hasRole, getSellerKeySet };
