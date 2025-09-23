// ===== 공통 헬퍼 =====
async function columnNames(table) {
  const q = await db.query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema='public' AND table_name=$1`,
    [table]
  );
  return new Set(q.rows.map(r => r.column_name));
}

// ===== 수정된 fuzzy 매칭 =====
async function findFuzzy(brand, code, limit = 5) {
  await ensureExt();
  const regs = await getKnownSpecsTables();
  const out = [];

  for (const r of regs) {
    const cols = await columnNames(r.specs_table);

    // 유사도 비교에 사용할 후보 표현식 (존재하는 컬럼만)
    const exprs = [`similarity(code_norm, lower($2))`];
    if (cols.has('display_name')) exprs.push(`similarity(lower(display_name), lower($2))`);
    if (cols.has('series'))       exprs.push(`similarity(lower(series), lower($2))`);

    // 최소 한 개는 항상 존재(code_norm)하므로 GREATEST가 비지 않습니다.
    const sql = `
      SELECT *,
             1.0 - GREATEST(${exprs.join(', ')}) AS score
        FROM public.${r.specs_table}
       WHERE brand_norm = lower($1)
       ORDER BY score ASC NULLS LAST
       LIMIT $3`;

    const q = await db.query(sql, [brand, code, limit]);
    for (const row of q.rows) {
      out.push({ table: r.specs_table, family_slug: r.family_slug, row, score: row.score ?? 0.9 });
    }
  }
  out.sort((a, b) => (a.score || 1) - (b.score || 1));
  return out.slice(0, limit);
}

// ===== 수정된 대체품 검색(룰 기반 fallback) =====
async function getAlternatives(table, baseRow, k = 8) {
  // 임베딩 유사도는 기존 로직 유지 (있으면 가장 먼저 시도)
  try {
    if (!baseRow.embedding) {
      await updateRowEmbedding(table, baseRow);
      const ref = await db.query(
        `SELECT embedding FROM public/${table} WHERE brand_norm=$1 AND code_norm=$2`,
        [baseRow.brand_norm, baseRow.code_norm]
      );
      baseRow.embedding = ref.rows[0]?.embedding || null;
    }
  } catch {}

  if (baseRow.embedding) {
    const q = await db.query(
      `SELECT *, (embedding <=> $1::vector) AS dist
         FROM public/${table}
        WHERE NOT (brand_norm=$2 AND code_norm=$3)
        ORDER BY embedding <=> $1::vector
        LIMIT $4`,
      [baseRow.embedding, baseRow.brand_norm, baseRow.code_norm, k]
    );
    return { mode: 'embedding', items: q.rows };
  }

  // 존재 컬럼만 사용하는 룰 기반 스코어
  const cols = await columnNames(table);

  // family_slug 비교는 대부분 있으므로 유지
  const familyTerm = `CASE WHEN family_slug IS NOT NULL AND family_slug = $1 THEN 0 ELSE 1 END`;

  // coil_voltage_vdc 가 있는 테이블에서만 가중치 반영, 없으면 1.0(중립)
  const coilTerm = cols.has('coil_voltage_vdc')
    ? `COALESCE(ABS(COALESCE(coil_voltage_vdc,0) - COALESCE($2::numeric,0)) / 100.0, 1.0)`
    : `1.0`;

  const sql2 = `
    SELECT *,
           (${familyTerm})*1.0 + ${coilTerm} AS score
      FROM public.${table}
     WHERE NOT (brand_norm=$3 AND code_norm=$4)
     ORDER BY score ASC
     LIMIT $5`;

  const q2 = await db.query(
    sql2,
    [baseRow.family_slug || null, baseRow.coil_voltage_vdc || null, baseRow.brand_norm, baseRow.code_norm, k]
  );
  return { mode: 'rule-fallback', items: q2.rows };
}
