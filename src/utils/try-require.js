'use strict';

module.exports = function tryRequire(candidates = [], options = {}) {
  const list = Array.isArray(candidates) ? candidates : [];
  const attempts = [];
  for (const mod of list) {
    try {
      return require(mod);
    } catch (err) {
      if (err && err.code === 'MODULE_NOT_FOUND' && typeof err.message === 'string' && err.message.includes(mod)) {
        attempts.push(err.message || String(err));
        continue;
      }
      throw err;
    }
  }
  if (options.silent) return {};
  const error = new Error(`MODULE_NOT_FOUND: ${list.join(' | ')}`);
  error.code = 'MODULE_NOT_FOUND';
  error.attempts = attempts;
  throw error;
};
