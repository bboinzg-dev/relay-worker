// @ts-check
'use strict';

const CANON = {
  coil_voltage: 'coil_voltage_vdc',
  rated_coil_voltage: 'coil_voltage_vdc',
  rated_coil_voltage_dc: 'coil_voltage_vdc',
  coil_voltage_value: 'coil_voltage_vdc',
  coil_voltage_code: 'coil_voltage_vdc',
  terminal_shape: 'mount_type',
  contact_current_code: 'contact_rating_dc_a',
  construction: 'package',
  poles: 'contact_form',
};

/**
 * @param {Record<string, unknown>} [domains]
 * @param {string[]} [allowedKeys]
 */
function normalizeVariantDomains(domains = {}, allowedKeys = []) {
  const allow = new Set(
    (Array.isArray(allowedKeys) ? allowedKeys : [])
      .map((s) => String(s).toLowerCase())
      .filter(Boolean),
  );
  const out = {};
  for (const [rawKey, rawValue] of Object.entries(domains || {})) {
    const key = String(rawKey || '').trim();
    if (!key) continue;
    const lower = key.toLowerCase();
    const base = CANON[lower] || (lower.includes('coil_voltage') ? 'coil_voltage_vdc' : lower);
    if (allow.size && !allow.has(base)) continue;
    const values = Array.isArray(rawValue) ? rawValue.slice() : [rawValue];
    out[base] = values;
  }
  if (out.coil_voltage_vdc) {
    const flat = new Set();
    for (const val of out.coil_voltage_vdc) {
      const base = String(val ?? '').trim().toUpperCase();
      if (base) {
        flat.add(base);
      }
      const decoded = decodeCoilVoltageVdcKeepBoth(val);
      if (decoded && typeof decoded === 'object' && decoded.vdc) {
        const vdc = String(decoded.vdc ?? '').trim();
        if (vdc) {
          flat.add(vdc);
        }
      }
    }
    out.coil_voltage_vdc = Array.from(flat);
  }
  return out;
}

function decodeCoilVoltageVdcKeepBoth(value) {
  const raw = String(value ?? '').toUpperCase().trim();
  if (!raw) return raw;
  // allow DC prefix and V/VDC suffix; allow 1~3 digits and 1H/4H
  const m = raw.match(/^(?:DC)?\s*((?:\d{1,3}(?:\.\d+)?)|[14]H)\s*(?:V(?:DC)?)?$/i);
  const token = (m ? m[1] : null) || raw;
  let val = token;
  if (token === '1H') {
    val = 1.5;
  } else if (token === '4H') {
    val = 4.5;
  } else if (/^\d{2,3}$/.test(token)) {
    const num = Number(token);
    if (Number.isFinite(num)) {
      val = num;
    }
  } else if (/^\d$/.test(token)) {
    val = Number(token);
  }
  return { raw, vdc: String(val) };
}

module.exports = {
  normalizeVariantDomains,
  decodeCoilVoltageVdcKeepBoth,
};
