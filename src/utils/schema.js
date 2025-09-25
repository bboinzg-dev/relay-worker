const db = require('./db'); // 기존과 동일 가정

// 식별자 안전화: 소문자 + 영문/숫자/언더스코어만
function safeIdent(name) {
  return String(name || '').replace(/[^a-zA-Z0-9_]/g, '').toLowerCase();
}

/**
 * 안전 업서트: (brand_norm, code_norm) 기준
 * - values의 키를 소문자로 정규화 + 중복 제거
 * - brand/code는 함수 인자만 사용하고 values에서는 제거
 * - 테이블에 "존재하는 컬럼"만 INSERT/UPDATE
 * - GENERATED ALWAYS/변경 금지 컬럼(id/created_at/updated_at/brand_norm/code_norm)은 UPDATE 제외
 */
async function upsertByBrandCode(tableName, { brand, code, ...values }) {
  const table = safeIdent(tableName);

  // norm 자동 세팅
  const payload = {
    brand,
    code,
    brand_norm: brand ? String(brand).toLowerCase() : null,
    code_norm : code  ? String(code ).toLowerCase() : null,
    ...values
  };

  // 테이블 메타
  const meta = await db.query(
    `select column_name, is_generated
       from information_schema.columns
      where table_schema='public' and table_name=$1`,
    [table]
  );
  const colsAllowed   = new Set(meta.rows.map(r => String(r.column_name).toLowerCase()));
  const generatedCols = new Set(
    meta.rows
      .filter(r => String(r.is_generated || '').toUpperCase() === 'ALWAYS')
      .map(r => String(r.column_name).toLowerCase())
  );

  // values 키 정규화(소문자) + 중복 제거
  const basePairs = Object.entries(payload).map(([k, v]) => [safeIdent(k), v]);

  const seen = new Set();
  const insertCols = [];
  const insertVals = [];
  for (const [k, v] of basePairs) {
    if (!k) continue;
    // brand/code는 여기서도 허용(함수 인자에서 온 값), values에 중복으로 있더라도 dedupe됨
    if (seen.has(k)) continue;
    if (!colsAllowed.has(k)) continue;     // 존재하지 않는 컬럼은 건너뜀
    if (generatedCols.has(k)) continue;    // GENERATED ALWAYS 제외
    seen.add(k);
    insertCols.push(k);
    insertVals.push(v);
  }
  if (!insertCols.length) return null;

  const NO_UPDATE = new Set(['id','created_at','updated_at','brand_norm','code_norm']);
  const params  = insertCols.map((_, i) => `$${i+1}`);
  const updates = insertCols
    .filter(c => !NO_UPDATE.has(c))
    .map(c => `${c}=EXCLUDED.${c}`);

  const sql = `
    insert into public.${table} (${insertCols.join(',')})
    values (${params.join(',')})
    on conflict (brand_norm, code_norm)
    do update set ${[...updates,'updated_at=now()'].join(', ')}
    returning *`;
  const res = await db.query(sql, insertVals);
  return res.rows?.[0] || null;
}

module.exports = { upsertByBrandCode /* , ...기존 export들 */ };
