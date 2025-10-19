'use strict';

function isValidCode(value) {
  const v = String(value || '').trim();
  if (!v) return false;
  if (v.length < 2 || v.length > 64) return false;
  if (!/[0-9A-Za-z]/.test(v)) return false;
  if (/\s{2,}/.test(v)) return false;
  if (/^pdf-?1(\.\d+)?$/i.test(v)) return false;
  return true;
}

module.exports = { isValidCode }; 
