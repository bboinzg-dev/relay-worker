const { v4: uuidv4 } = require('uuid');
let parseActor = null;
try { parseActor = require('./auth').parseActor; } catch { parseActor = () => ({}) }

function nowMs() { const [s,n] = process.hrtime(); return s*1000 + n/1e6; }

function requestLogger() {
  return function reqLogger(req, res, next) {
    const start = nowMs();
    const reqId = req.headers['x-request-id'] || uuidv4();
    res.setHeader('x-request-id', reqId);
    const actor = parseActor(req) || {};
    res.locals.__actor = actor;
    res.locals.__reqId = reqId;

    const logBase = {
      req_id: reqId,
      severity: 'INFO',
      httpRequest: {
        requestMethod: req.method,
        requestUrl: req.originalUrl || req.url,
        userAgent: req.headers['user-agent'],
        remoteIp: req.ip,
        protocol: req.protocol,
      },
      labels: {
        route: (req.route && req.route.path) || undefined,
        actor_id: actor.id || undefined,
        tenant_id: actor.tenantId || undefined,
      },
    };

    // log start
    console.log(JSON.stringify({ ...logBase, event: 'request_start' }));

    function finish() {
      res.removeListener('finish', finish);
      res.removeListener('close', finish);
      const dur = nowMs() - start;
      const payload = {
        ...logBase,
        event: 'request_end',
        httpRequest: { ...logBase.httpRequest, status: res.statusCode, latency: `${Math.round(dur)}ms` },
        duration_ms: Math.round(dur),
      };
      if (res.statusCode >= 500) payload.severity = 'ERROR';
      console.log(JSON.stringify(payload));
    }

    res.on('finish', finish);
    res.on('close', finish);

    next();
  };
}

function logError(err, ctx={}) {
  const payload = {
    severity: 'ERROR',
    event: 'app_error',
    message: err && (err.stack || err.message || String(err)),
    ...ctx,
  };
  console.error(JSON.stringify(payload));
}

function patchDbLogging(db) {
  if (!db || typeof db.query !== 'function') return;
  const orig = db.query.bind(db);
  db.query = async function patchedQuery(sql, params) {
    const t0 = nowMs();
    try {
      const r = await orig(sql, params);
      const ms = Math.round(nowMs() - t0);
      const head = String(sql || '').trim().split(/\s+/).slice(0, 6).join(' ');
      console.log(JSON.stringify({ severity:'DEBUG', event:'sql', duration_ms: ms, head, rows: r?.rowCount }));
      return r;
    } catch (e) {
      const ms = Math.round(nowMs() - t0);
      console.error(JSON.stringify({ severity:'ERROR', event:'sql_error', duration_ms: ms, head: String(sql||'').slice(0,120), error: e.message || String(e) }));
      throw e;
    }
  };
}

module.exports = { requestLogger, logError, patchDbLogging };
