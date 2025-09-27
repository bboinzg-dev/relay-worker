'use strict';

const crypto = require('crypto');

function generateRunId() {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  return crypto.randomBytes(16).toString('hex');
}

module.exports = { generateRunId };