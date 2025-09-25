// --- 안전한 식별자(테이블/컬럼) → 소문자 고정 ---
function safeIdent(name) {
  return String(name || '').replace(/[^a-zA-Z0-9_]/g, '').toLowerCase();
}

/**
 * Upsert by (brand_norm, code_norm) with case-insensitive dedupe
 * - 컬럼명: 소문자 정규화 + 중복 제거
 * - 존재하지 않는 컬럼/GENERATED 컬럼은 INSERT/UPDATE에서 제외
 * - brand_norm/code_norm 자동 보정
 */
async function upsertByBrandCode(tableName, values) {
  const safeTable = safeIdent(tableName);
  const payload = { ...(values || {}) };

  // legacy 호환: norm 자동 세팅
  if (payload.brand && payload.brand_norm == null) payload.brand_norm = String(payload.brand).toLowerCase();
  if (payload.code  && payload.code_norm  == null) payload.code_norm  = String(payload.code ).toLowerCase();

  // 테이블 메타: 존재 컬럼 & GENERATED 컬럼 수집
  const meta = await db.query(
    `select column_name, is_generated
       from information_schema.columns
      where table_schema='public' and table_name=$1`,
    [safeTable]
  );
  const colsAllowed   = new Set((meta.rows || []).map(r => String(r.column_name).toLowerCase()));
  const generatedCols = new Set((meta.rows || [])
    .filter(r => String(r.is_generated || '').toUpperCase() === 'ALWAYS')
    .map(r => String(r.column_name).toLowerCase())
  );

  // (컬럼, 값) 페어 생성: 키는 소문자 안전식별자
  const pairs = Object.entries(payload).map(([k, v]) => [safeIdent(k), v]);

  // 소문자 기준 중복 제거 + 필터링
  const seen = new Set();
  const insertCols = [];
  const insertVals = [];
  for (const [k, v] of pairs) {
    if (!k) continue;
    if (seen.has(k)) continue;             // 중복 제거 (code vs CODE)
    if (!colsAllowed.has(k)) continue;     // 존재하지 않는 컬럼은 스킵
    if (generatedCols.has(k)) continue;    // GENERATED ALWAYS 컬럼은 스킵
    seen.add(k);
    insertCols.push(k);
    insertVals.push(v);
  }
  if (!insertCols.length) return null;

  // 업데이트에서 제외할 컬럼
  const NO_UPDATE = new Set(['id', 'created_at', 'updated_at', 'brand_norm', 'code_norm']);
  const params  = insertCols.map((_, i) => `$${i + 1}`);
  const updates = insertCols.filter(c => !NO_UPDATE.has(c)).map(c => `${c}=EXCLUDED.${c}`);

  const sql = `
    insert into public.${safeTable} (${insertCols.join(',')})
    values (${params.join(',')})
    on conflict (brand_norm, code_norm)
    do update set ${[...updates, 'updated_at=now()'].join(', ')}
    returning *`;

  const res = await db.query(sql, insertVals);
  return (res.rows && res.rows[0]) || null;
}
