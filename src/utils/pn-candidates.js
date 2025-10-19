'use strict';

const ORDERING_HEAD_RE = /^(code|part|pn|p\/n|型番|品番|型號|형명|품번|주문|ordering|order|type)/i;
const CODE_RE = /[A-Z0-9](?:[A-Z0-9._\/-]*[A-Z0-9]){5,}/g;

function uniq(list) {
  const seen = new Set();
  const out = [];
  for (const entry of Array.isArray(list) ? list : []) {
    const str = String(entry || '').trim();
    if (!str) continue;
    const key = str.toUpperCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(str);
  }
  return out;
}

function isGarbage(token) {
  if (!token) return true;
  const t = String(token).trim();
  if (t.length < 5 || t.length > 48) return true;
  if (/\s{2,}/.test(t)) return true;
  if (/^http/i.test(t) || /[:@]/.test(t)) return true;
  if (/^(PAGE|P)\s*\d+$/i.test(t)) return true;
  if (/^ASCTB\d{3,4}[A-Z](?:\s+\d{6})?$/i.test(t)) return true;
  const alphas = (t.match(/[A-Z]/g) || []).length;
  const digits = (t.match(/\d/g) || []).length;
  if (alphas < 1 || digits < 1) return true;
  return false;
}

function fromOrderingText(text, extractOrderingInfo) {
  try {
    if (!extractOrderingInfo || !text) return [];
    const res = extractOrderingInfo(String(text), 200);
    return Array.isArray(res?.codes) ? res.codes : [];
  } catch {
    return [];
  }
}

function fromDocAiTables(docai) {
  const tables = docai && Array.isArray(docai.tables) ? docai.tables : [];
  const out = [];
  for (const table of tables) {
    const rows = Array.isArray(table.rows)
      ? table.rows
      : Array.isArray(table.cells)
        ? [table.cells]
        : [];
    if (!rows.length) continue;
    const headerRow = Array.isArray(table.headers) ? table.headers : rows[0];
    const header = (headerRow || []).map((cell) => String(cell?.text || cell || '').trim());
    let codeIdx = -1;
    for (let i = 0; i < header.length; i += 1) {
      if (ORDERING_HEAD_RE.test(header[i])) {
        codeIdx = i;
        break;
      }
    }
    for (const row of rows) {
      const cells = Array.isArray(row) ? row : [];
      const targets = codeIdx >= 0 ? [cells[codeIdx]] : cells;
      for (const cell of targets) {
        const text = String(cell?.text || cell || '');
        const matches = text.match(CODE_RE) || [];
        for (const hit of matches) {
          out.push(hit.toUpperCase());
        }
      }
    }
  }
  return out;
}

function collectPnCandidates({ docText, docai, extractOrderingInfo }) {
  const ordering = fromOrderingText(docText, extractOrderingInfo);
  const tableCodes = fromDocAiTables(docai);
  const merged = uniq([...ordering, ...tableCodes]).filter((code) => !isGarbage(code));
  return { codes: merged, debug: { fromOrdering: ordering.length, fromTables: tableCodes.length } };
}

module.exports = { collectPnCandidates };
