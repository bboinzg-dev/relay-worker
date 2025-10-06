'use strict';

const { randomUUID } = require('node:crypto');

function generateRunId(prefix = '') {
  // 기본은 uuid v4. 필요하면 prefix 붙여서 사용 가능.
  const id = randomUUID();
  return prefix ? `${prefix}_${id}` : id;
}

module.exports = { generateRunId };