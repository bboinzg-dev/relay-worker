'use strict';

/**
 * Fetch the latest FX rate for the given currency. Falls back to monthly table
 * when daily data is not available. Returns KRW=1 when the requested currency
 * is KRW or falsy.
 *
 * @param {{ query(text: string, params?: any[]): Promise<{ rows: any[], rowCount: number }> }} pg
 *   pg Pool/Client compatible instance.
 * @param {string} currency
 */
async function fetchFx(pg, currency) {
  const curr = String(currency || 'KRW').toUpperCase();
  const now = new Date();
  if (curr === 'KRW') {
    return {
      rate: 1,
      yyyymm: Number(`${now.getUTCFullYear()}${String(now.getUTCMonth() + 1).padStart(2, '0')}`),
      source: 'KRW=1',
    };
  }

  const sqlDaily = `
    SELECT rate, to_char(rate_date, 'YYYYMM')::int AS yyyymm, source
      FROM public.fx_rates_daily
     WHERE provider = 'koreaexim' AND currency = $1
     ORDER BY rate_date DESC
     LIMIT 1
  `;
  const daily = await pg.query(sqlDaily, [curr]);
  if (daily.rowCount) {
    return daily.rows[0];
  }

  const sqlMonthly = `
    SELECT rate, yyyymm, source
      FROM public.fx_rates_monthly
     WHERE provider = 'koreaexim' AND currency = $1
     ORDER BY yyyymm DESC
     LIMIT 1
  `;
  const monthly = await pg.query(sqlMonthly, [curr]);
  if (monthly.rowCount) {
    return monthly.rows[0];
  }

  throw new Error(`fx_rate_not_found_${curr}`);
}

/**
 * Convert unit price into KRW cents with 10 KRW rounding rule.
 *
 * @param {number|string} amount      Unit amount in the original currency.
 * @param {string} currency           Currency code (ISO alpha-3).
 * @param {number|string} fxRate      FX rate used for conversion.
 * @returns {number|null}
 */
function toKrwCentsRounded10(amount, currency, fxRate) {
  const raw = Number(amount);
  if (!Number.isFinite(raw) || raw <= 0) {
    return null;
  }
  const curr = String(currency || 'KRW').toUpperCase();
  if (curr === 'KRW') {
    const won10 = Math.round(raw / 10) * 10;
    return Math.max(0, Math.round(won10 * 100));
  }

  const rate = Number(fxRate);
  if (!Number.isFinite(rate) || rate <= 0) {
    return null;
  }
  const won = raw * rate;
  const wonRounded10 = Math.round(won / 10) * 10;
  return Math.max(0, Math.round(wonRounded10 * 100));
}

module.exports = { fetchFx, toKrwCentsRounded10 };

