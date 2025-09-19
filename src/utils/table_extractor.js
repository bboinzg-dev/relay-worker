// relay-worker/src/utils/table_extractor.js
'use strict';

const HEAD_SYNS = {
  code:   ['type','type no','part','part no','part number','model','model no','catalog no','品番','型番'],
  series: ['series','family','ラインナップ','形名'],
  desc:   ['description','remarks','note','仕様','備考'],
};

function normalizeHeader(h) {
  const s = h.toLowerCase().replace(/\s+/g,' ').trim();
  for (const [key, syns] of Object.entries(HEAD_SYNS)) {
    for (const v of syns) if (s.includes(v)) return key;
  }
  return 'other';
}

/** DocAI JSON → 행배열 {code, series, desc, raw}  */
function parseDocAiTables(doc) {
  const out = [];
  const tables = doc?.pages?.flatMap(p => p.tables || []) || [];
  for (const t of tables) {
    const head = (t.headerRows?.[0]?.cells || []).map(c => c.layout?.textAnchor?.content || c.content || '');
    const cols = head.map(normalizeHeader);
    if (!cols.length) continue;

    for (const row of (t.bodyRows || [])) {
      const cells = row.cells || [];
      const rec = { code:'', series:'', desc:'', raw:[] };
      for (let i=0;i<cells.length;i++) {
        const val = cells[i]?.layout?.textAnchor?.content || cells[i]?.content || '';
        rec.raw.push(val.trim());
        const key = cols[i] || 'other';
        if (key === 'code'  && !rec.code)   rec.code   = val.trim();
        if (key === 'series'&& !rec.series) rec.series = val.trim();
        if (key === 'desc')                 rec.desc  = (rec.desc? rec.desc+' ' : '') + val.trim();
      }
      if (rec.code) out.push(rec);
    }
  }
  return dedupRows(out);
}

function dedupRows(rows) {
  const seen = new Set();
  const ret = [];
  for (const r of rows) {
    const k = `${r.code}::${r.series}`;
    if (seen.has(k)) continue;
    seen.add(k);
    ret.push(r);
  }
  return ret;
}

module.exports = { parseDocAiTables };
