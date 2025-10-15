'use strict';

const rawPerKeyLimit = Number(process.env.VARIANT_DOMAIN_MAX_PER_KEY);
const PER_KEY_DOMAIN_MAX = Number.isFinite(rawPerKeyLimit) && rawPerKeyLimit > 0
  ? Math.floor(rawPerKeyLimit)
  : 8;
const rawMaxCombos = Number(process.env.VARIANT_COMBOS_MAX);
const MAX_COMBOS = Number.isFinite(rawMaxCombos) && rawMaxCombos > 0
  ? Math.floor(rawMaxCombos)
  : 5000;

const LIST_SEP = /[\s,;/|·•]+/;
const VOLTAGE_UNIT_RE = /\d\s*(?:V|VAC|VDC)\b/i;
// 패턴리스 정규화기: 1A/1B/1C/2A/2B/2C/1A1B/2AB 등 조합 처리
function normalizeContactForm(value) {
  if (value == null) return null;
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw == null) return null;
  let s = String(raw).normalize('NFKC').toLowerCase();
  if (!s.trim()) return null;
  s = s.replace(/[\s\-_/]/g, '');
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
    const rawCount = m[1] ? Number.parseInt(m[1], 10) : 1;
    const n = Number.isFinite(rawCount) && rawCount > 0 ? rawCount : 1;
    const letters = m[2] || '';
    if (letters.includes('c')) c = Math.max(c, n);
    if (letters.includes('a')) a = Math.max(a, n);
    if (letters.includes('b')) b = Math.max(b, n);
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
  const limit = Number.isFinite(MAX_COMBOS) && MAX_COMBOS > 0 ? MAX_COMBOS : Infinity;
  let out = [[]];
  for (const list of lists) {
    const next = [];
    for (const a of out) {
      for (const b of list) {
        if (next.length >= limit) return next;
        next.push(a.concat([b]));
      }
    }
    out = next;
    if (!out.length) break;
  }
  return out;
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
    s = s.replace(/[^0-9]/g, '');
      continue;
    }
    if (lower === 'num') {
      const match = s.match(/-?\d+(?:\.\d+)?/);
      s = match ? match[0] : '';
      continue;
    }
    if (lower.startsWith('pad:')) {
      const parts = op.split(':');
      const width = Number(parts[1]) || 2;
      const fillRaw = parts.length > 2 ? parts[2] : '';
      const fill = fillRaw && fillRaw.trim() ? fillRaw.trim()[0] : '0';
      s = s.padStart(width, fill);
      continue;
    }
    if (lower.startsWith('pad=')) {
      const parts = op.split('=');
      const width = Number(parts[1]) || 2;
      const fillRaw = parts.length > 2 ? parts[2] : '';
      const fill = fillRaw && fillRaw.trim() ? fillRaw.trim()[0] : '0';
      s = s.padStart(width, fill);
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

function escapeRegex(str) {
  return String(str || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function templateNeedsPlaceholder(tpl, key) {
  if (!tpl || !key) return false;
  const pattern = escapeRegex(String(key).trim());
  if (!pattern) return false;
  const re = new RegExp(`\{\{?\\s*${pattern}(?:\\W|\}|\\|)`, 'i');
  return re.test(String(tpl));
}

function defaultTextContainsExact(text, pn) {
  if (!text || !pn) return false;
  const pattern = escapeRegex(String(pn).trim());
  if (!pattern) return false;
  const re = new RegExp(`(^|[^A-Za-z0-9])${pattern}(?=$|[^A-Za-z0-9])`, 'i');
  return re.test(String(text));
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
  const haystackInput = options.haystack;
  const haystack = Array.isArray(haystackInput)
    ? haystackInput.filter((chunk) => typeof chunk === 'string' && chunk.trim()).join('\n')
    : typeof haystackInput === 'string'
      ? haystackInput
      : '';
  const textContainsExactFn =
    typeof options.textContainsExact === 'function' ? options.textContainsExact : defaultTextContainsExact;
  const previewOnly = Boolean(options.previewOnly);
  const maxTemplateAttemptsSource =
    options.maxTemplateAttempts ?? process.env.MAX_TEMPLATE_ATTEMPTS ?? 200;
  const maxTemplateAttemptsRaw = Number(maxTemplateAttemptsSource);
  const maxTemplateAttempts = Number.isFinite(maxTemplateAttemptsRaw) && maxTemplateAttemptsRaw > 0
    ? Math.floor(maxTemplateAttemptsRaw)
    : null;
  const onTemplateRender = typeof options.onTemplateRender === 'function' ? options.onTemplateRender : null;

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
      const normed = normalizeList(fromExact).slice(0, PER_KEY_DOMAIN_MAX);
      return normed.length ? normed : [fromExact];
    }
    const lower = key.toLowerCase();
    if (values[lower] != null) {
      const normed = normalizeList(values[lower]).slice(0, PER_KEY_DOMAIN_MAX);
      return normed.length ? normed : [values[lower]];
    }
    return [null];
  });

  const combos = lists.length ? cartesian(lists) : [[]];
  const mpnCandidates = collectMpnSeeds(base);

  const rows = [];
  let templateAttempts = 0;
  for (let idx = 0; idx < combos.length; idx += 1) {
    if (maxTemplateAttempts && templateAttempts >= maxTemplateAttempts) break;
    const combo = combos[idx];
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
    const normalizedCoilVoltageVdc = normalizeCoilVoltage(rowValues.coil_voltage_vdc);
    if (normalizedCoilVoltageVdc) assignValue(rowValues, 'coil_voltage_vdc', normalizedCoilVoltageVdc);
    else {
      delete rowValues.coil_voltage_vdc;
      delete rowValues.coil_voltagevdc;
    }

    const normalizedCoilVoltageVac = normalizeCoilVoltage(rowValues.coil_voltage_vac);
    if (normalizedCoilVoltageVac) assignValue(rowValues, 'coil_voltage_vac', normalizedCoilVoltageVac);
    else {
      delete rowValues.coil_voltage_vac;
      delete rowValues.coil_voltagevac;
    }

    const needsCoil = pnTemplate
      && (templateNeedsPlaceholder(pnTemplate, 'coil_voltage_vdc')
        || templateNeedsPlaceholder(pnTemplate, 'coil_voltage_vac'));
    const needsContact = pnTemplate && templateNeedsPlaceholder(pnTemplate, 'contact_form');
    const needsTerm = pnTemplate && templateNeedsPlaceholder(pnTemplate, 'terminal_shape');
    const needsOp = pnTemplate && templateNeedsPlaceholder(pnTemplate, 'operating_function');
    const needsPack = pnTemplate && templateNeedsPlaceholder(pnTemplate, 'packing_style');

    const hasCoil = Boolean(
      normalizeCoilVoltage(rowValues.coil_voltage_vdc ?? rowValues.coil_voltage_vac),
    );
    const hasContact = Boolean(rowValues.contact_form ?? rowValues.contactform);
    const hasTerm = Boolean(
      rowValues.terminal_shape
        ?? rowValues.terminalshape
        ?? rowValues.terminal_form
        ?? rowValues.terminalform,
    );
    const hasOp = Boolean(rowValues.operating_function ?? rowValues.operatingfunction);
    const hasPack = Boolean(rowValues.packing_style ?? rowValues.packingstyle);

    let generatedByTemplate = false;
    let code = null;
    let attempted = false;

    if (pnTemplate) {
      if (!normalizedSeries) continue;
      if (needsCoil && !hasCoil) continue;
      if (needsContact && !hasContact) continue;
      if (needsTerm && !hasTerm) continue;
      if (needsOp && !hasOp) continue;
      if (needsPack && !hasPack) continue;

      const context = {
        ...rowValues,
        series: normalizedSeries,
        series_code: normalizedSeries,
        contact_form: normalizedContactForm,
        coil_voltage_vdc: normalizedCoilVoltageVdc,
        coil_voltage_vac: normalizedCoilVoltageVac,
      };

      const rendered = renderTemplate(pnTemplate, context);
      attempted = true;
      templateAttempts += 1;
      const renderedCode = typeof rendered === 'string' ? rendered.trim() : String(rendered || '').trim();
      if (!renderedCode) {
        if (onTemplateRender) onTemplateRender({ accepted: false, reason: 'empty_render', context });
        continue;
      }
      if (needsCoil && !VOLTAGE_UNIT_RE.test(renderedCode)) {
        if (onTemplateRender) {
          onTemplateRender({
            accepted: false,
            reason: 'missing_voltage_unit',
            candidate: renderedCode,
            context,
          });
        }
        continue;
      }
      if (haystack && !textContainsExactFn(haystack, renderedCode)) {
        if (onTemplateRender) {
          onTemplateRender({
            accepted: false,
            reason: 'missing_doc_evidence',
            candidate: renderedCode,
            context,
          });
        }
        continue;
      }
      code = renderedCode;
      generatedByTemplate = true;
            if (onTemplateRender) {
        onTemplateRender({ accepted: true, candidate: renderedCode, context });
      }
    } else if (mpnCandidates[idx]) {
      code = mpnCandidates[idx];
    } else if (mpnCandidates.length) {
      code = mpnCandidates[0];
    } else {
      // 템플릿/표 후보가 없으면 "임의 PN"을 만들지 않는다 (가짜 PN 차단)
      continue;
    }

    code = String(code || '').trim();
    if (!code) continue;

    if (generatedByTemplate && !isLikelyPn(code)) continue;

    const codeNorm = code.toLowerCase();
    if (rows.some((r) => r.code_norm === codeNorm)) continue;

    if (!previewOnly) {
      rows.push({
        code,
        code_norm: codeNorm,
        values: rowValues,
      });
    }

    if (maxTemplateAttempts && attempted && templateAttempts >= maxTemplateAttempts) break;
  }

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