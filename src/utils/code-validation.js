'use strict';

const PANASONIC_DOC_RE = /^(?:ASCTB|DS|JL|JCQ|JM|LT|SLL|SC|SS|XT|STB|LL|PY|CH|CE)[A-Z0-9-]*$/;
const PANASONIC_DOC_WITH_DATE_RE = /ASCTB\d{3,4}[A-Z]\s+\d{6}/i;

function isValidCode(value) {
  const v = String(value || '').trim();
  if (!v) return false;
  if (v.length < 2 || v.length > 64) return false;
  if (!/[0-9A-Za-z]/.test(v)) return false;
  if (/\s{2,}/.test(v)) return false;
  if (/^pdf-?1(\.\d+)?$/i.test(v)) return false;
  return true;
}

function looksLikeGarbageCode(value, brand) {
  const raw = value == null ? '' : String(value);
  if (!raw) return false;

  if (
    /^[a-f0-9]{20,}_\d{10,}/i.test(raw) ||
    /(^|_)(mech|doc|pdf)[-_]/i.test(raw) ||
    /pdf:|\.pdf$/i.test(raw)
  ) {
    return true;
  }

  const brandKey = String(brand || '').trim().toLowerCase();
  const isPanasonic =
    brandKey === 'panasonic' ||
    brandKey === 'matsushita' ||
    brandKey === 'nais' ||
    brandKey === 'panasonic industry';

  if (!isPanasonic) return false;

  const upper = raw.trim().toUpperCase();
  if (!upper) return false;

  if (PANASONIC_DOC_RE.test(upper)) return true;
  if (PANASONIC_DOC_WITH_DATE_RE.test(raw)) return true;

  return false;
}

module.exports = { isValidCode, looksLikeGarbageCode };
