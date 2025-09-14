const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const db = require('./src/utils/db');

const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: '10mb' }));

async function getRegistry() {
  const r = await db.query(`SELECT family_slug, specs_table FROM public.component_registry ORDER BY family_slug`);
  return r.rows;
}
async function getColumns(table) {
  const r = await db.query(`
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='public' AND table_name=$1
  `, [table]);
  const set = new Set(r.rows.map(x => x.column_name));
  return set;
}

function parseListParam(v) {
  if (!v) return [];
  if (Array.isArray(v)) return v.flatMap(parseListParam);
  return String(v).split(',').map(s => s.trim()).filter(Boolean);
}

function buildAggListingsWith(tableAlias='s'){
  return `LEFT JOIN (
    SELECT brand_norm, code_norm,
           MIN(price_cents) AS min_price_cents,
           SUM(quantity_available) AS total_qty,
           MIN(lead_time_days) AS min_lead_days
    FROM public.listings
    GROUP BY brand_norm, code_norm
  ) ls ON ls.brand_norm = ${tableAlias}.brand_norm AND ls.code_norm = ${tableAlias}.code_norm`;
}

// Build and run per-table query, returning rows + facet pre-agg
async function searchOneTable(table, cols, { q, filters, sort, limitPerTable=200 }) {
  // Mapping: only include columns that exist
  const has = (c) => cols.has(c);
  const selectPieces = [
    `s.brand`, `s.code`,
    has('display_name') ? `s.display_name` : `NULL AS display_name`,
    has('family_slug') ? `s.family_slug` : `NULL AS family_slug`,
    has('series') ? `s.series` : `NULL AS series`,
    has('contact_form') ? `s.contact_form` : `NULL AS contact_form`,
    has('coil_voltage_vdc') ? `s.coil_voltage_vdc` : `NULL AS coil_voltage_vdc`,
    has('datasheet_url') ? `s.datasheet_url` : `NULL AS datasheet_url`,
    has('cover') ? `s.cover` : `NULL AS cover`,
    has('updated_at') ? `s.updated_at` : `NULL AS updated_at`,
    `s.brand_norm`, `s.code_norm`,
    `ls.min_price_cents`, `ls.total_qty`, `ls.min_lead_days`,
    // relevance signals using pg_trgm similarity; guard when q empty
    q ? `GREATEST(similarity(s.brand_norm, $q), similarity(s.code_norm, $q), similarity(COALESCE(lower(s.display_name),''), $q)) AS score` : `0.0 AS score`
  ];
  const where = [];
  const params = {};
  if (q) {
    params.$q = q.toLowerCase();
    where.push(`(s.brand_norm ILIKE '%'||$q||'%' OR s.code_norm ILIKE '%'||$q||'%' OR COALESCE(lower(s.display_name),'') ILIKE '%'||$q||'%')`);
  }
  if (filters.families?.length && has('family_slug')) {
    params.$families = filters.families.map(x=>x.toLowerCase());
    where.push(`lower(s.family_slug) = ANY($families)`);
  }
  if (filters.brands?.length) {
    params.$brands = filters.brands.map(x=>x.toLowerCase());
    where.push(`s.brand_norm = ANY($brands)`);
  }
  if (filters.series?.length && has('series')) {
    params.$series = filters.series.map(x=>x.toLowerCase());
    where.push(`lower(s.series) = ANY($series)`);
  }
  if (filters.contact_form?.length && has('contact_form')) {
    params.$cforms = filters.contact_form.map(x=>x.toLowerCase());
    where.push(`lower(s.contact_form) = ANY($cforms)`);
  }
  if (has('coil_voltage_vdc')) {
    if (filters.coil_v_min != null) { params.$vmin = Number(filters.coil_v_min); where.push(`s.coil_voltage_vdc >= $vmin`); }
    if (filters.coil_v_max != null) { params.$vmax = Number(filters.coil_v_max); where.push(`s.coil_voltage_vdc <= $vmax`); }
  }
  // Build ORDER BY
  let orderBy = `score DESC NULLS LAST, ls.min_price_cents ASC NULLS LAST, s.updated_at DESC NULLS LAST`;
  switch ((sort||'relevance').toLowerCase()) {
    case 'price_asc': orderBy = `ls.min_price_cents ASC NULLS LAST, s.updated_at DESC NULLS LAST`; break;
    case 'price_desc': orderBy = `ls.min_price_cents DESC NULLS FIRST, s.updated_at DESC NULLS LAST`; break;
    case 'leadtime_asc': orderBy = `ls.min_lead_days ASC NULLS LAST, ls.min_price_cents ASC NULLS LAST`; break;
    case 'updated_desc': orderBy = `s.updated_at DESC NULLS LAST`; break;
    default: break;
  }
  const sql = `
    SELECT ${selectPieces.join(', ')}
    FROM public.${table} s
    ${buildAggListingsWith('s')}
    ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
    ORDER BY ${orderBy}
    LIMIT ${Math.max(1, Math.min(1000, limitPerTable))}
  `;
  // Convert named params to array
  const names = Object.keys(params);
  const vals = names.map(k => params[k]);
  let qSql = sql;
  names.forEach((name, idx) => { qSql = qSql.replaceAll(name, `$${idx+1}`); });
  const r = await db.query(qSql, vals);

  // facet pre-agg
  const facets = {
    brand: {},
    series: {},
    family: {},
    contact_form: {},
    coil_voltage_vdc: { min: null, max: null }
  };
  for (const row of r.rows) {
    const b = String(row.brand || '').trim(); if (b) facets.brand[b] = (facets.brand[b]||0)+1;
    const s = row.series ? String(row.series).trim() : '';
    if (s) facets.series[s] = (facets.series[s]||0)+1;
    const f = row.family_slug ? String(row.family_slug).trim() : '';
    if (f) facets.family[f] = (facets.family[f]||0)+1;
    const cf = row.contact_form ? String(row.contact_form).trim() : '';
    if (cf) facets.contact_form[cf] = (facets.contact_form[cf]||0)+1;
    if (row.coil_voltage_vdc != null) {
      const v = Number(row.coil_voltage_vdc);
      facets.coil_voltage_vdc.min = facets.coil_voltage_vdc.min==null? v : Math.min(facets.coil_voltage_vdc.min, v);
      facets.coil_voltage_vdc.max = facets.coil_voltage_vdc.max==null? v : Math.max(facets.coil_voltage_vdc.max, v);
    }
  }
  return { rows: r.rows, facets };
}

function mergeFacets(a, b){
  const out = JSON.parse(JSON.stringify(a));
  function plusCount(obj, add){
    for (const [k,v] of Object.entries(add)) obj[k] = (obj[k]||0) + (v||0);
  }
  plusCount(out.brand, b.brand);
  plusCount(out.series, b.series);
  plusCount(out.family, b.family);
  plusCount(out.contact_form, b.contact_form);
  out.coil_voltage_vdc.min = out.coil_voltage_vdc.min==null? b.coil_voltage_vdc.min : (b.coil_voltage_vdc.min==null? out.coil_voltage_vdc.min : Math.min(out.coil_voltage_vdc.min, b.coil_voltage_vdc.min));
  out.coil_voltage_vdc.max = out.coil_voltage_vdc.max==null? b.coil_voltage_vdc.max : (b.coil_voltage_vdc.max==null? out.coil_voltage_vdc.max : Math.max(out.coil_voltage_vdc.max, b.coil_voltage_vdc.max));
  return out;
}

app.get('/parts/searchx', async (req, res) => {
  try {
    const q = (req.query.q || '').toString().trim();
    const page = Math.max(1, parseInt(req.query.page || '1', 10));
    const limit = Math.max(1, Math.min(100, parseInt(req.query.limit || '24', 10)));
    const sort = (req.query.sort || 'relevance').toString();

    const filters = {
      families: parseListParam(req.query.family),
      brands: parseListParam(req.query.brand),
      series: parseListParam(req.query.series),
      contact_form: parseListParam(req.query.contact_form),
      coil_v_min: req.query.coil_v_min!=null && req.query.coil_v_min!=='' ? Number(req.query.coil_v_min) : null,
      coil_v_max: req.query.coil_v_max!=null && req.query.coil_v_max!=='' ? Number(req.query.coil_v_max) : null
    };

    const reg = await getRegistry();
    let allRows = [];
    let facets = { brand:{}, series:{}, family:{}, contact_form:{}, coil_voltage_vdc:{min:null,max:null} };

    for (const r of reg) {
      // family filter short-circuit
      if (filters.families.length && !filters.families.includes(String(r.family_slug||'').toLowerCase())) continue;
      const cols = await getColumns(r.specs_table);
      const out = await searchOneTable(r.specs_table, cols, { q, filters, sort });
      allRows = allRows.concat(out.rows.map(row => ({ ...row, _table: r.specs_table })));
      facets = mergeFacets(facets, out.facets);
    }

    // global sort (relevance / price / lead / updated)
    function key(row) {
      switch (sort) {
        case 'price_asc': return [row.min_price_cents==null? Number.MAX_SAFE_INTEGER : Number(row.min_price_cents), -new Date(row.updated_at||0).getTime()];
        case 'price_desc': return [row.min_price_cents==null? -1 : -Number(row.min_price_cents), -new Date(row.updated_at||0).getTime()];
        case 'leadtime_asc': return [row.min_lead_days==null? Number.MAX_SAFE_INTEGER : Number(row.min_lead_days), row.min_price_cents==null? Number.MAX_SAFE_INTEGER : Number(row.min_price_cents)];
        case 'updated_desc': return [-new Date(row.updated_at||0).getTime()];
        default: return [-(Number(row.score||0)), row.min_price_cents==null? Number.MAX_SAFE_INTEGER : Number(row.min_price_cents), -new Date(row.updated_at||0).getTime()];
      }
    }
    allRows.sort((a,b)=>{
      const ka = key(a), kb = key(b);
      for (let i=0;i<Math.max(ka.length,kb.length);i++){
        const va = ka[i]||0, vb = kb[i]||0;
        if (va<vb) return -1; if (va>vb) return 1;
      }
      return 0;
    });

    const total = allRows.length;
    const start = (page-1)*limit;
    const items = allRows.slice(start, start+limit);

    // top facets
    function topKV(obj, n=20) {
      const arr = Object.entries(obj||{}).sort((a,b)=>b[1]-a[1]).slice(0,n);
      return arr.map(([k,v])=>({ key:k, count:v }));
    }
    const facetPayload = {
      brand: topKV(facets.brand, 50),
      series: topKV(facets.series, 50),
      family: topKV(facets.family, 50),
      contact_form: topKV(facets.contact_form, 20),
      coil_voltage_vdc: facets.coil_voltage_vdc
    };

    res.json({
      query: { q, page, limit, sort, filters },
      total,
      items,
      facets: facetPayload
    });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: String(e.message || e) });
  }
});

module.exports = app;
