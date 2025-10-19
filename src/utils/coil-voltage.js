'use strict';

function decodeCoilVoltageVdc(pnOrCode = '') {
  const s = String(pnOrCode ?? '').trim();
  if (!s) return null;
  // anywhere matcher: 1~3 digits or 1H/4H, optional V/VDC around it
  const m = s.match(/(?:^|[^0-9A-Za-z])((?:[14]H)|(?:\d{1,3}(?:\.\d+)?))\s*(?:V(?:DC)?)?(?:$|[^0-9A-Za-z])/i);
  if (m) {
    const tok = String(m[1] || '').toUpperCase();
    if (tok === '1H') return 1.5;
    if (tok === '4H') return 4.5;
    const n = Number(tok);
    if (Number.isFinite(n)) {
      if (n >= 100) return n % 10 === 0 ? n / 10 : n;
      return n;
    }
  }
  return null;
}

module.exports = { decodeCoilVoltageVdc };