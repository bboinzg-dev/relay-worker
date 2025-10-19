'use strict';

const ALIASES = new Map([
  ['contact arrangement', 'contact_arrangement'],
  ['contact configuration', 'contact_arrangement'],
  ['configuration', 'contact_arrangement'],
  ['mounting type', 'mount_type'],
  ['mounting', 'mount_type'],
  ['terminal form', 'terminal_shape'],
  ['terminal type', 'terminal_shape'],
  ['terminal shape', 'terminal_shape'],
  ['packing', 'packing_style'],
  ['package type', 'package'],
]);

function sanitizeKey(key) {
  let str = String(key || '').trim();
  if (!str) return '';

  let prefix = '';
  const leading = str.match(/^_+/);
  if (leading) {
    prefix = leading[0];
    str = str.slice(prefix.length);
  }

  if (!str) return prefix.toLowerCase();

  const camelConverted = str.replace(/([a-z0-9])([A-Z])/g, '$1_$2');
  const sanitized = camelConverted.replace(/[^a-zA-Z0-9_]/g, '_');
  const collapsed = sanitized.replace(/__+/g, '_').replace(/^_+|_+$/g, '');
  const final = collapsed ? `${prefix}${collapsed}` : prefix;
  return final.toLowerCase();
}

function normalizeSpecKey(key) {
  if (key == null) return '';
  const trimmed = String(key).trim();
  if (!trimmed) return '';
  const alias = ALIASES.get(trimmed.toLowerCase());
  if (alias) return sanitizeKey(alias);
  return sanitizeKey(trimmed);
}

module.exports = { normalizeSpecKey };
