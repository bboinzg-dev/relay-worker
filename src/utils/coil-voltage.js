'use strict';

function decodeCoilVoltageVdc(pnOrCode = '') {
  const s = String(pnOrCode ?? '').trim();
  if (!s) return null;

  const matchNumericTail = /([0-9]{2,3})$/.exec(s);
  if (matchNumericTail) {
    const n = Number(matchNumericTail[1]);
    if (Number.isFinite(n)) {
      if (n >= 100) {
        return n % 10 === 0 ? n / 10 : n;
      }
      if (n === 45) return 4.5;
      if (n === 15) return 1.5;
      if (n === 48) return 48;
      return n;
    }
  }

  const hMatch = /([14])H$/i.exec(s);
  if (hMatch) {
    return hMatch[1] === '1' ? 1.5 : 4.5;
  }

  return null;
}

module.exports = { decodeCoilVoltageVdc };