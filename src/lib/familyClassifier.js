'use strict';
const { Client } = require('pg');
const { GoogleGenAI } = require('@google/genai');

const MODEL = process.env.TEXT_EMBED_MODEL || 'gemini-embedding-001';
const OUTPUT_DIM = parseInt(process.env.TEXT_EMBED_DIM || '768', 10);

// 튜닝 파라미터(환경변수로도 오버라이드 가능)
const W_SIM      = parseFloat(process.env.FAM_SCORE_W_SIM || '1.0');   // 임베딩 유사도 가중
const W_HINT     = parseFloat(process.env.FAM_SCORE_W_HINT || '0.08');  // UI 힌트 보너스
const MARGIN_MIN = parseFloat(process.env.FAM_SCORE_MARGIN || '0.05');  // top1 - top2 최소 마진
const MAX_K_RETRY = 1;                                                  // 재분류 1회

function cosine(a, b) {
  // a, b are arrays (L2 normalized 전제 X) -> 내적 / (||a||*||b||)
  let dot = 0, na = 0, nb = 0;
  for (let i = 0; i < a.length; i++) { const x=a[i], y=b[i]; dot+=x*y; na+=x*x; nb+=y*y; }
  if (!na || !nb) return 0;
  return dot / (Math.sqrt(na)*Math.sqrt(nb));
}

function parsePgvector(text) {
  // pg returns like '[0.12,0.34,...]' or '(..)' 형식 → 숫자배열
  const s = text.replace(/[\[\]\(\)]/g, '');
  if (!s.trim()) return [];
  return s.split(',').map(Number);
}

function keywordBoost(text, rules = [], negatives = []) {
  const t = text.toLowerCase();
  let score = 0;
  for (const r of rules) {
    const pat = new RegExp(r.pattern, 'i');
    if (pat.test(t)) score += Number(r.weight || 0);
  }
  for (const r of negatives) {
    const pat = new RegExp(r.pattern, 'i');
    if (pat.test(t)) score -= Number(r.weight || 0);
  }
  return score;
}

/**
 * familyHint: 'auto' | 'relay_signal' | 'relay_power' | ...
 * confusionSetIfRelay: 릴레이 계열 재시도용 접두어 매칭
 */
async function decideFamily({ db, docText, familyHint = 'auto' }) {
  // 1) 문서 임베딩 (Query용)
  const ai = new GoogleGenAI({});
  const resp = await ai.models.embedContent({
    model: MODEL,
    contents: docText,
    // 분류/검색 안정화를 위해 Query 최적화
    config: { taskType: 'SEMANTIC_SIMILARITY', outputDimensionality: OUTPUT_DIM }
  });
  const qvec = resp.embeddings[0].values;

  // 2) 후보 로딩(임베딩+키워드)
  const { rows } = await db.query(`
    SELECT d.family_slug,
           d.confusion_group,
           d.embedding::text AS emb,
           d.keywords,
           d.negative_keywords
      FROM public.component_family_descriptor d
     WHERE d.embedding IS NOT NULL
  `);

  if (!rows.length) throw new Error('No family embeddings. Run embed seeding first.');

  function scoreAll(candidates) {
    const scored = [];
    for (const r of candidates) {
      const emb = parsePgvector(r.emb);
      const sim = cosine(qvec, emb);                // [-1,1]
      const kw  = keywordBoost(docText, r.keywords || [], r.negative_keywords || []);
      const hint = (familyHint !== 'auto' && familyHint === r.family_slug) ? W_HINT : 0;
      const total = (W_SIM * sim) + kw + hint;      // 하이브리드 점수
      scored.push({ family_slug: r.family_slug, sim, kw, hint, score: total });
    }
    scored.sort((a,b) => b.score - a.score);
    return scored;
  }

  // 3) 1차 스코어링
  let scored = scoreAll(rows);
  let top1 = scored[0], top2 = scored[1];

  // 4) 마진 부족 시, "릴레이 계열" 제한 재분류(1회)
  if ((top1.score - top2.score) < MARGIN_MIN && top1.confusion_group) {
    const sameGroup = rows.filter(r => r.confusion_group === top1.confusion_group);
    const rescored = scoreAll(sameGroup);
    const t1 = rescored[0], t2 = rescored[1];
    if (t1 && (t1.score - (t2?.score ?? -999)) >= (top1.score - top2.score)) {
      scored = rescored; top1 = t1; top2 = t2;
    }
  }

  // 5) 최종 매핑: component_registry → specs_table
  const map = await db.query(
    `SELECT specs_table FROM public.component_registry WHERE family_slug = $1 LIMIT 1`,
    [top1.family_slug]
  );
  const table = map.rows[0]?.specs_table;

  return {
    family: top1.family_slug,
    specs_table: table,
    margin: (top1.score - (top2?.score ?? -999)),
    debug: { top3: scored.slice(0,3) }
  };
}

module.exports = { decideFamily };
