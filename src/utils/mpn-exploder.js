'use strict';

const LIST_SEP = /[\s,;/|·•]+/;
const CONTACT_FORM_PATTERNS = [
  { regex: /(1\s*form\s*a|1a|spst-?no)/i, value: '1A' },
  { regex: /(2\s*form\s*a|2a|dpst-?no)/i, value: '2A' },
];
const CONTACT_FORM_ALLOWED = new Set(['1A', '2A']);
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

function normalizeContactForm(value) {
  if (value == null) return null;
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw == null) return null;
  const str = String(raw).trim();
  if (!str) return null;
  for (const { regex, value: mapped } of CONTACT_FORM_PATTERNS) {
    if (regex.test(str)) return mapped;
  }
  const compact = str.replace(/\s+/g, '').toUpperCase();
  if (CONTACT_FORM_ALLOWED.has(compact)) return compact;
  if (compact === 'SPST' || compact === 'SPSTNO') return '1A';
  if (compact === 'DPST' || compact === 'DPSTNO') return '2A';
  return null;
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

function renderTemplate(tpl, ctx) {
  if (!tpl) return '';
  return tpl.replace(/\{\{([^}]+)\}\}/g, (_, expr) => {
    const [head, ...pipes] = expr.split('|').map((s) => s.trim());
    let val = ctx[head];
    for (const pipe of pipes) {
      if (pipe === 'upper') val = String(val ?? '').toUpperCase();
      else if (pipe === 'lower') val = String(val ?? '').toLowerCase();
      else if (pipe.startsWith('pad=')) {
        const n = parseInt(pipe.slice(4), 10);
        const str = String(val ?? '');
        val = Number.isFinite(n) ? str.padStart(n, '0') : str;
      }
    }
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

    const normalizedCoilVoltage = normalizeCoilVoltage(rowValues.coil_voltage_vdc);
    if (normalizedCoilVoltage) assignValue(rowValues, 'coil_voltage_vdc', normalizedCoilVoltage);
    else {
      delete rowValues.coil_voltage_vdc;
      delete rowValues.coil_voltagevdc;
    }

    const canUseTemplate = pnTemplate
      && normalizedSeries
      && normalizedContactForm
      && CONTACT_FORM_ALLOWED.has(normalizedContactForm)
      && normalizedCoilVoltage;

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
};