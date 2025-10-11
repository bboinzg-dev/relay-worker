'use strict';

const LIST_SEP = /[\s,;/|·•]+/;
// 패턴리스 정규화기: 1A/1B/1C/2A/2B/2C/1A1B/2AB 등 조합 처리
function normalizeContactForm(value) {
  if (value == null) return null;
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw == null) return null;
  let s = String(raw).normalize('NFKC').toLowerCase();
  if (!s.trim()) return null;
  s = s.replace(/[\s\-_]/g, '');
  // 동의어 치환
  s = s
    .replace(/spstnc/g, '1b')
    .replace(/spst(no)?/g, '1a')
    .replace(/dpst(no)?/g, '2a')
    .replace(/spdt/g, '1c')
    .replace(/dpdt/g, '2c')
    .replace(/form/g, '');
  // (\d+)?[abc]+ 블록들을 누적 카운트
  let a = 0;
  let b = 0;
  let c = 0;
  const re = /(\d+)?([abc]+)/g;
  let m;
  while ((m = re.exec(s)) !== null) {
    const n = m[1] ? parseInt(m[1], 10) : 1;
    const letters = m[2];
    if (letters.includes('c')) c += n;
    if (letters.includes('a')) a += n;
    if (letters.includes('b')) b += n;
  }
  if (!a && !b && !c) return null;
  if (c > 0) return `${c}C`;
  if (a > 0 && b > 0) return `${a}A${b}B`;
  if (a > 0) return `${a}A`;
  return `${b}B`;
}
const SERIES_STRIP_WORDS = /\b(relays?|series|relay|power|signal)\b/gi;
const NON_MPN_WORDS = new Set([
  'relay', 'relays', 'coil', 'vdc', 'vac', 'form', 'series', 'typ', 'max', 'min'
]);

function normalizeSeriesCode(value) {
  if (value == null) return null;
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw == null) return null;
  const str = String(raw).trim();
  if (!str) return null;
  const cleaned = str
    .replace(SERIES_STRIP_WORDS, '')
    .replace(/\s+/g, '')
    .trim();
  const upper = cleaned.toUpperCase();
  return upper || null;
}


function normalizeCoilVoltage(value) {
  if (value == null) return null;
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw == null) return null;
  const str = String(raw).trim();
  if (!str) return null;
  const digits = str.match(/\d+/g);
  if (!digits || !digits.length) return null;
  const joined = digits.join('');
  if (!joined) return null;
  return joined;
}

function isLikelyPn(value) {
  if (value == null) return false;
  const str = String(value).trim();
  if (!str) return false;
  return /^[A-Z]{1,4}[A-Z0-9\-_/]{2,}$/i.test(str);
}

function derivePrefix(token) {
  if (!token) return '';
  const trimmed = token.trim();
  if (!trimmed) return '';

  // 가장 흔한 패턴: 마지막 구분자(-/_/) 직전까지를 prefix로 본다.
  const delimiterMatch = trimmed.match(/^(.*[\-_/])[A-Za-z0-9]+$/);
  if (delimiterMatch) return delimiterMatch[1];

  // 구분자가 없으면 최초의 영문자로 시작하는 시퀀스를 prefix로 사용
  const headMatch = trimmed.match(/^([A-Za-z][A-Za-z0-9]*)/);
  if (headMatch) return headMatch[1];

  return '';
}

function normalizeSuffix(token) {
  if (!token) return '';
  return token.replace(/^[\-_/]+/, '');
}

function splitAndCarryPrefix(raw) {
  if (!raw) return [];
  const tokens = String(raw)
    .split(LIST_SEP)
    .map((t) => t.trim())
    .filter(Boolean);

  const out = [];
  let lastPrefix = '';

  for (const token of tokens) {
    const lower = token.toLowerCase();
    if (NON_MPN_WORDS.has(lower)) continue;

    const leading = token[0];
    const isLeadingDelimiter = leading != null && /[\-_/]/.test(leading);

    if ((/^[0-9]/.test(token) || isLeadingDelimiter) && lastPrefix) {
      const suffix = normalizeSuffix(token);
      if (suffix && /[A-Za-z]/.test(suffix)) {
        out.push(lastPrefix + suffix);
        continue;
      }
    }

    out.push(token);
    const prefix = derivePrefix(token);
    if (prefix) lastPrefix = prefix;
  }

  const seen = new Set();
  const deduped = [];
  for (const token of out) {
    const norm = token.toLowerCase();
    if (seen.has(norm)) continue;
    seen.add(norm);
    deduped.push(token);
  }
  return deduped;
}

function normalizeList(value) {
  if (value == null) return [];
  if (Array.isArray(value)) return value.flatMap(normalizeList);
  if (typeof value === 'string') {
    return value
      .split(LIST_SEP)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [value];
}

function cartesian(lists) {
  return lists.reduce(
    (acc, list) => acc.flatMap((a) => list.map((b) => a.concat([b]))),
    [[]]
  );
}

function __applyOps(val, ops = []) {
  const first = Array.isArray(val) ? val[0] : val;
  let s = first == null ? '' : String(first);
  for (const rawOp of ops) {
    if (!rawOp) continue;
    const opToken = rawOp.includes('=') ? rawOp.replace('=', ':') : rawOp;
    const op = opToken.trim();
    if (!op) continue;
    const lower = op.toLowerCase();
    if (lower === 'upper') {
      s = s.toUpperCase();
      continue;
    }
    if (lower === 'lower') {
      s = s.toLowerCase();
      continue;
    }
    if (lower === 'first') {
      s = s.split(',')[0].trim();
      continue;
    }
    if (lower === 'alnum') {
      s = s.replace(/[^0-9A-Z]/gi, '');
      continue;
    }
    if (lower === 'digits') {
      const digits = s.match(/\d+/g) || [''];
      s = digits.join('');
      continue;
    }
    if (lower === 'num') {
      const match = s.match(/-?\d+(?:\.\d+)?/);
      s = match ? match[0] : '';
      continue;
    }
    if (lower.startsWith('pad:')) {
      const [, widthRaw] = op.split(':');
      const width = Number(widthRaw) || 2;
      s = s.padStart(width, '0');
      continue;
    }
    if (lower.startsWith('pad=')) {
      const [, widthRaw] = op.split('=');
      const width = Number(widthRaw) || 2;
      s = s.padStart(width, '0');
      continue;
    }
    if (lower.startsWith('slice:')) {
      const parts = op.split(':');
      const start = Number(parts[1]) || 0;
      const end = parts.length > 2 && parts[2] !== '' ? Number(parts[2]) : undefined;
      s = s.slice(start, Number.isNaN(end) ? undefined : end);
      continue;
    }
    if (lower.startsWith('map:')) {
      const mapPairs = op.slice(4).split(',');
      const mapping = Object.create(null);
      for (const pair of mapPairs) {
        const [from, to] = pair.split('>');
        if (!from || to == null) continue;
        mapping[String(from).trim().toUpperCase()] = String(to).trim();
      }
      const key = String(s).trim().toUpperCase();
      s = mapping[key] ?? s;
      continue;
    }
  }
  return s;
}

function renderTemplate(tpl, ctx) {
  if (!tpl) return '';
  return String(tpl).replace(/\{\{?([^{}]+)\}\}?/g, (_, expr) => {
    const [head, ...pipes] = String(expr)
      .split('|')
      .map((s) => s.trim());
    if (!head) return '';
    const val = __applyOps(ctx[head], pipes);
    return val == null ? '' : String(val);
  });
}

function assignValue(target, key, value) {
  if (!key) return;
  target[key] = value;
  const lower = String(key).toLowerCase();
  if (lower && lower !== key) target[lower] = value;
}

function collectMpnSeeds(base) {
  const seeds = [];
  const seen = new Set();

  const push = (val) => {
    if (val == null) return;
    if (Array.isArray(val)) {
      for (const item of val) push(item);
      return;
    }
    if (typeof val === 'string') {
      const trimmed = val.trim();
      if (!trimmed) return;
      const parts = splitAndCarryPrefix(trimmed);
      if (parts.length > 1) {
        for (const part of parts) push(part);
        return;
      }
      const norm = trimmed.toLowerCase();
      if (seen.has(norm)) return;
      seen.add(norm);
      seeds.push(trimmed);
      return;
    }
    const str = String(val);
    const norm = str.toLowerCase();
    if (seen.has(norm)) return;
    seen.add(norm);
    seeds.push(str);
  };

  push(base?.mpn);
  if (base?.code) push(base.code);

  const extraKeys = ['mpn', 'part_number', 'part_no', 'code'];
  const values = base && typeof base.values === 'object' ? base.values : {};
  for (const key of extraKeys) push(values[key]);
  return seeds;
}

function explodeToRows(base, options = {}) {
  const variantKeys = Array.isArray(options.variantKeys)
    ? options.variantKeys.map((k) => String(k || '').trim()).filter(Boolean)
    : [];
  const pnTemplate = options.pnTemplate || null;

  const values = {};
  if (base && typeof base.values === 'object') {
    for (const [rawKey, rawValue] of Object.entries(base.values)) {
      const key = String(rawKey || '').trim();
      if (!key) continue;
      assignValue(values, key, rawValue);
    }
  }
  if (base?.series && values.series == null) assignValue(values, 'series', base.series);
  if (base?.series_code && values.series_code == null) assignValue(values, 'series_code', base.series_code);

  const lists = variantKeys.map((key) => {
    const fromExact = values[key];
    if (fromExact != null) {
      const normed = normalizeList(fromExact);
      return normed.length ? normed : [fromExact];
    }
    const lower = key.toLowerCase();
    if (values[lower] != null) {
      const normed = normalizeList(values[lower]);
      return normed.length ? normed : [values[lower]];
    }
    return [null];
  });

  const combos = lists.length ? cartesian(lists) : [[]];
  const mpnCandidates = collectMpnSeeds(base);

  const rows = [];
  combos.forEach((combo, idx) => {
    const rowValues = { ...values };
    variantKeys.forEach((key, keyIdx) => {
      const val = combo[keyIdx];
      if (val == null || val === '') return;
      assignValue(rowValues, key, val);
    });

    const normalizedSeries = normalizeSeriesCode(
      rowValues.series_code
        ?? rowValues.series
        ?? base?.series_code
        ?? base?.series
        ?? null,
    );
    if (normalizedSeries) {
      assignValue(rowValues, 'series_code', normalizedSeries);
      if (rowValues.series == null) assignValue(rowValues, 'series', normalizedSeries);
    }

    const normalizedContactForm = normalizeContactForm(
      rowValues.contact_form
        ?? rowValues.contact_arrangement
        ?? rowValues.form
        ?? null,
    );
    if (normalizedContactForm) assignValue(rowValues, 'contact_form', normalizedContactForm);
    else {
      delete rowValues.contact_form;
      delete rowValues.contactform;
    }

        assignValue(rowValues, 'coil_voltage_text', rowValues.coil_voltage_vdc);
    const normalizedCoilVoltage = normalizeCoilVoltage(rowValues.coil_voltage_vdc);
    if (normalizedCoilVoltage) assignValue(rowValues, 'coil_voltage_vdc', normalizedCoilVoltage);
    else {
      delete rowValues.coil_voltage_vdc;
      delete rowValues.coil_voltagevdc;
    }

    const canUseTemplate = pnTemplate && normalizedSeries && normalizedCoilVoltage;

    let generatedByTemplate = false;
    let generatedByFallback = false;
    let code = null;

    if (pnTemplate) {
  if (!canUseTemplate) return;
      code = renderTemplate(pnTemplate, {
        ...rowValues,
        series: normalizedSeries,
        series_code: normalizedSeries,
        contact_form: normalizedContactForm,
        coil_voltage_vdc: normalizedCoilVoltage,
      });
            generatedByTemplate = true;
    } else if (mpnCandidates[idx]) {
      code = mpnCandidates[idx];
    } else if (mpnCandidates.length) {
      code = mpnCandidates[0];
    } else {
      const parts = [];
      if (normalizedSeries) parts.push(normalizedSeries);
      else if (base?.series) parts.push(base.series);
      else if (base?.series_code) parts.push(base.series_code);
      const suffix = variantKeys
        .map((key) => rowValues[key] ?? rowValues[key?.toLowerCase()])
        .filter((v) => v != null && v !== '')
        .map((v) => String(v))
        .join('');
      if (suffix) parts.push(suffix);
      code = parts.join('');
            generatedByFallback = true;
    }

    code = String(code || '').trim();
    if (!code) return;

        if ((generatedByTemplate || generatedByFallback) && !isLikelyPn(code)) return;

    const codeNorm = code.toLowerCase();
    if (rows.some((r) => r.code_norm === codeNorm)) return;

    rows.push({
      code,
      code_norm: codeNorm,
      values: rowValues,
    });
  });

  return rows;
}

module.exports = {
  LIST_SEP,
  NON_MPN_WORDS,
  splitAndCarryPrefix,
  normalizeList,
  cartesian,
  renderTemplate,
  explodeToRows,
  normalizeContactForm,
};