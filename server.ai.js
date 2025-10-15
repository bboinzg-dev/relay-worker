// server.ai.js — Express Router export (mounted at /api/ai/*)
'use strict';

const express = require('express');
const router = express.Router();
const db = require('./db');
const { getTableForFamily } = require('./src/lib/registry');
const { VertexAI } = require('@google-cloud/vertexai');

function getVertex() {
  const project = process.env.GCP_PROJECT_ID || process.env.GOOGLE_CLOUD_PROJECT;
  const location = process.env.VERTEX_LOCATION || 'asia-northeast3';
  if (!project) throw new Error('GCP_PROJECT_ID/GOOGLE_CLOUD_PROJECT is required');
  return new VertexAI({ project, location });
}

// 숫자 파싱: '20 A', '2A', '20A/125VAC' → 20 (numeric)
function numExpr(pgTextIdent) {
  // regexp_replace(text, '[^0-9.]', '', 'g')::numeric
  return `COALESCE(NULLIF(regexp_replace(${pgTextIdent}, '[^0-9.]', '', 'g'), '')::numeric, NULL)`;
}

// LLM에 물어볼 스키마 (최소화)
const SEARCH_PLAN_SCHEMA = [
  '{',
  ' "family": string|null,              // ex: "relay_power", "relay_signal", "resistor_chip", etc.',
  ' "brand_like": string|null,          // brand substring',
  ' "pn_like": string|null,             // part-number substring',
  ' "must": string[],                   // keywords that MUST appear in text fields',
  ' "numeric": [                        // numeric filters',
  '   { "key": string, "op": ">="|"<="|"="|">"|"<", "value": number, "unit": string|null }',
  ' ]',
  ' // Examples:',
  ' // "20A 이상인 릴레이" -> family=relay_power or relay_signal, numeric=[{key:"contact_rating_a", op:">=", value:20, unit:"A"}]',
  ' // "소형 전등용 저항"   -> family=resistor_chip, must=["lamp","lighting"]',
  '}'
].join('\n');

async function callGeminiToPlan(q) {
  const client = getVertex();
  const mdl = client.getGenerativeModel({
    model: process.env.VERTEX_MODEL_ID || 'gemini-2.5-flash',
    systemInstruction: {
      parts: [{ text:
        'You convert Korean natural-language electronic component queries into a strict JSON search plan.\n' +
        'Follow this JSON schema exactly:\n' + SEARCH_PLAN_SCHEMA + '\n' +
        'Return ONLY JSON. Family must be one of known slugs if obvious: ' +
        '["relay_power","relay_signal","relay_reed","relay_ssr","resistor_chip","mosfet","capacitor_mlcc","capacitor_elec","tvs_diode","op_amp","comparator", "inductor_power"].'
      }]
    },
    generationConfig: { temperature: 0.1, responseMimeType: 'application/json', maxOutputTokens: 1024 }
  });

  const resp = await mdl.generateContent({
    contents: [{ role: 'user', parts: [{ text: String(q) }]}]
  });
  let plan = {};
  try { plan = JSON.parse(resp?.response?.candidates?.[0]?.content?.parts?.[0]?.text || '{}'); }
  catch { plan = {}; }
  // 기본값 보정
  if (!Array.isArray(plan.must)) plan.must = [];
  if (!Array.isArray(plan.numeric)) plan.numeric = [];
  plan.family = (plan.family || '').trim() || null;
  plan.brand_like = (plan.brand_like || '').trim() || null;
  plan.pn_like = (plan.pn_like || '').trim() || null;
  return plan;
}

// family별 컬럼 alias(자주 쓰는 것만)
const FAMILY_ALIAS = {
  relay_power: {
    contactA: ['contact_rating_a', 'contact_rating_dc_a', 'contact_rating_ac_a', 'contact_rating_text'],
    coilV:    ['coil_voltage_vdc', 'rated_coil_voltage_dc', 'coil_voltage_text', 'coil_voltage_code'],
  },
  relay_signal: {
    contactA: ['contact_rating_a', 'contact_rating_dc_a', 'contact_rating_ac_a', 'contact_rating_text'],
    coilV:    ['coil_voltage_vdc', 'coil_voltage_text', 'coil_voltage_code'],
  },
  resistor_chip: {
    ohm: ['resistance_ohm'],
  },
  mosfet: {
    ida: ['current_id_a'],
    vds: ['voltage_vds_v'],
    rds: ['rds_on_mohm'],
  },
};

// numeric 키를 실제 컬럼 후보로 확장
function expandNumericKey(family, key) {
  const a = FAMILY_ALIAS[family] || {};
  if (a[key]) return a[key];
  return [key]; // 그대로 시도
}

// 동적 필터 SQL 만들기
function buildFilterSQL({ table, family, plan, limit }) {
  const where = [];
  const args = [];
  let arg = 0;

    const baseTable = String(table || '').trim();
  if (!baseTable) throw new Error('table required');
  const qualifiedTable = baseTable.includes('.') ? baseTable : `public.${baseTable}`;

  // family 고정(안전)
  if (family) where.push(`(family_slug = $${++arg})`), args.push(family);

  // brand/pn like
  if (plan.brand_like) where.push(`(unaccent(brand)::text ILIKE unaccent('%' || $${++arg} || '%'))`), args.push(plan.brand_like);
  if (plan.pn_like)    where.push(`(unaccent(pn)::text    ILIKE unaccent('%' || $${++arg} || '%'))`),    args.push(plan.pn_like);

  // must 키워드: raw_json과 title 성격 필드에 or-축약 없이 AND로
  for (const k of plan.must) {
    const p = `%${k}%`;
    where.push(`( (raw_json::text ILIKE $${++arg}) OR (series ILIKE $${arg}) OR (contact_form ILIKE $${arg}) )`);
    args.push(p);
  }

  // numeric: 각 키마다 가능한 컬럼 후보 중 하나라도 만족하면 OK → (col1 OP v OR col2 OP v ...)
  for (const f of (plan.numeric || [])) {
    const op = (f.op || '>=')?.replace(/[^<=>]/g,'');
    const v  = Number(f.value);
    if (!isFinite(v)) continue;

    const cols = expandNumericKey(family, (f.key || '').trim());
    const pieces = [];
    for (const col of cols) {
      // 텍스트 컬럼도 있으니 numExpr로 변환
      pieces.push(`(${numExpr(`"${col}"`)} ${op} $${++arg})`);
      args.push(v);
    }
    if (pieces.length) where.push(`(${pieces.join(' OR ')})`);
  }

  const sql =
    `SELECT id, family_slug, brand, pn,
            COALESCE(NULLIF(brand,''),'') || CASE WHEN COALESCE(NULLIF(pn,''),'')<>'' THEN ' '||pn ELSE '' END AS title,
            image_uri, datasheet_url, series, updated_at
       FROM ${qualifiedTable}
      ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
      ORDER BY updated_at DESC
      LIMIT ${Math.min(Math.max(parseInt(limit,10)||24, 1), 100)}`;

  return { sql, args };
}

/* ---------- /api/ai/resolve : 브랜드/PN 추출 ---------- */
router.get('/resolve', async (req, res) => {
  try {
    const q = String(req.query?.q || req.body?.q || '').trim();
    if (!q) return res.status(400).json({ ok:false, error:'q required' });

    // 간단 규칙 우선
    const simple = /([A-Za-z][A-Za-z0-9.+-]+)\s+([A-Za-z0-9][A-Za-z0-9.+-]+)/.exec(q);
    if (simple) {
      const [, brand, code] = simple;
      return res.json({ ok:true, brand, code, source:'simple' });
    }

    // LLM로 추출 → brand_like / pn_like로 반환
    const plan = await callGeminiToPlan(q);
    if (plan.brand_like || plan.pn_like) {
      return res.json({ ok:true, brand: plan.brand_like || null, code: plan.pn_like || null, source:'ai' });
    }
    
    return res.json({ ok: true, echo: q });
  } catch (e) {
    return res.status(500).json({ ok:false, error:String(e?.message||e) });
  }
});

/* ---------- /api/ai/search : AI/PN 검색 ---------- */
// e.g. GET /api/ai/search?q=컨택트 정격 20A 릴레이&mode=ai
router.get('/search', async (req, res) => {
  try {
    const q = String(req.query?.q || '').trim();
    const mode = String(req.query?.mode || 'ai');
    const limit = String(req.query?.limit || '24');
    if (!q) return res.status(400).json({ ok:false, error:'q required' });

    // 1) pn 간단 검색 모드
    if (mode === 'pn') {
      const rows = await db.query(
        `SELECT id, family_slug, brand, pn,
                COALESCE(NULLIF(brand,''),'') || CASE WHEN COALESCE(NULLIF(pn,''),'')<>'' THEN ' '||pn ELSE '' END AS title,
                image_uri, datasheet_url, series, updated_at
           FROM public.component_specs
          WHERE unaccent(brand) ILIKE unaccent('%' || $1 || '%')
             OR unaccent(pn)    ILIKE unaccent('%' || $1 || '%')
          ORDER BY updated_at DESC
          LIMIT ${Math.min(Math.max(parseInt(limit,10)||24, 1), 100)}`,
        [q]
      );
      return res.json({ ok:true, explain:{ mode:'pn', q }, items: rows.rows });
    }

    // 2) AI 의미 검색 모드
    const plan = await callGeminiToPlan(q);

    // family 없으면 릴레이/저항 등 빈도 높은 후보로 시도 (간단 휴리스틱)
    const family = plan.family || (/릴레이/.test(q) ? 'relay_power' : /저항|resistor/i.test(q) ? 'resistor_chip' : null);

    // family가 있어야 스펙 필터를 정확히 적용 가능. 없으면 component_specs에 넓게 걸쳐서 must/brand/pn로 검색
    if (!family) {
      const rows = await db.query(
        `SELECT id, family_slug, brand, pn,
                COALESCE(NULLIF(brand,''),'') || CASE WHEN COALESCE(NULLIF(pn,''),'')<>'' THEN ' '||pn ELSE '' END AS title,
                image_uri, datasheet_url, series, updated_at
           FROM public.component_specs
          WHERE ($1::text IS NULL OR unaccent(brand) ILIKE unaccent('%' || $1::text || '%'))
            OR ($2::text IS NULL OR unaccent(pn)    ILIKE unaccent('%' || $2::text || '%'))
          ORDER BY updated_at DESC
          LIMIT ${Math.min(Math.max(parseInt(limit,10)||24, 1), 100)}`,
        [plan.brand_like || null, plan.pn_like || null]
      );
      return res.json({ ok:true, explain:{ mode:'ai-broad', plan }, items: rows.rows });
    }

    const table = await getTableForFamily(family);
    const { sql, args } = buildFilterSQL({ table, family, plan, limit });
    const rows = await db.query(sql, args);

    return res.json({
      ok:true,
      explain:{ mode:'ai', family, table, plan, sql_preview: sql.replace(/\s+/g,' ').slice(0,280) + '...' },
      items: rows.rows
    });
  } catch (e) {
    return res.status(500).json({ ok:false, error:String(e?.message||e) });
  }
});


router.get('/ping', (_req, res) => res.json({ ok:true }));

module.exports = router;