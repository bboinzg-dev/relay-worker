const { parseActor } = require('./auth');
function getTenant(req){
  try {
    const actor = parseActor(req);
    return (actor && actor.tenantId) ? String(actor.tenantId) : null;
  } catch { return null; }
}
function getActorId(req){
  try {
    const actor = parseActor(req);
    return (actor && actor.id) ? String(actor.id) : null;
  } catch { return null; }
}
function whereTenant(column='tenant_id', tenant){
  if (!tenant) return `(${column} IS NULL)`; // global rows only
  return `(${column} IS NULL OR ${column} = ${escapeLiteral(tenant)})`;
}
function escapeLiteral(v){
  return `'${String(v).replace(/'/g, "''")}'`;
}
module.exports = { getTenant, getActorId, whereTenant, escapeLiteral };
