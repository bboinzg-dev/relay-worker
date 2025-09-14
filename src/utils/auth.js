/**
 * Very light auth/tenancy parser.
 * Expect headers: x-actor-id, x-tenant-id, x-actor-roles (csv: admin,seller,buyer)
 * In production, replace with real JWT verification (e.g., Firebase Auth, custom JWT).
 */
function parseActor(req) {
  const id = (req.headers['x-actor-id'] || '').toString() || null;
  const tenantId = (req.headers['x-tenant-id'] || '').toString() || null;
  const rolesRaw = (req.headers['x-actor-roles'] || '').toString();
  const roles = rolesRaw ? rolesRaw.split(',').map(s => s.trim()) : [];
  return { id, tenantId, roles };
}
function requireRole(actor, role) {
  return actor && Array.isArray(actor.roles) && actor.roles.includes(role);
}
module.exports = { parseActor, requireRole };
