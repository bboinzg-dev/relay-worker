'use strict';
const { getPool } = require('../../db');

const TTL = Number(process.env.REGISTRY_CACHE_TTL_MS || 60_000);
let _cacheMap = null, _cacheExp = 0;

async function loadRegistryMap() {
  const now = Date.now();
  if (_cacheMap && now < _cacheExp) return _cacheMap;
  const { rows } = await getPool().query(
    `SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`
  );
  const map = new Map();
  for (const r of rows) {
    const fam = String(r.family_slug || '').trim().toLowerCase();
    const tbl = String(r.specs_table || '').trim();
    if (fam && /^[A-Za-z0-9_]+$/.test(tbl)) map.set(fam, tbl);
  }
  _cacheMap = map; _cacheExp = now + TTL;
  return map;
}

/** family → specs_table (없으면 기본 규칙 public.<fam>_specs) */
async function getTableForFamily(family) {
  const fam = String(family || '').trim().toLowerCase();
  if (!fam) throw new Error('family required');
  const map = await loadRegistryMap();
  const t = map.get(fam) || `public.${fam}_specs`;
  if (!/^[A-Za-z0-9_.]+$/.test(t)) throw new Error('Invalid table:' + t);
  return t;
}

/** 부팅 시: 블루프린트에 있는데 레지스트리에 없는 가족군 자동 보강 */
async function ensureRegistryFromBlueprints() {
  const pool = getPool();
  const fams = await pool.query(
    `SELECT family_slug FROM public.component_spec_blueprint`
  );
  const reg = await pool.query(
    `SELECT family_slug FROM public.component_registry`
  );
  const have = new Set(reg.rows.map(r => String(r.family_slug || '').trim().toLowerCase()));
  const toAdd = [];
  for (const r of fams.rows) {
    const fam = String(r.family_slug || '').trim().toLowerCase();
    if (!fam || have.has(fam)) continue;
    toAdd.push({ fam, table: `public.${fam}_specs` });
  }
  for (const it of toAdd) {
    await pool.query(
      `INSERT INTO public.component_registry (family_slug, specs_table)
       VALUES ($1,$2) ON CONFLICT (family_slug) DO NOTHING`,
      [it.fam, it.table]
    );
  }
  // 캐시 무효화
  _cacheMap = null; _cacheExp = 0;
  return { added: toAdd.length };
}

module.exports = { getTableForFamily, ensureRegistryFromBlueprints, loadRegistryMap };