// pseudo: src/routes/photo/analyze.ts
import { pool } from '../db'; // 기존 PG 커넥션
import { getNearestFromEmbedding, getBoxes } from '../services/vision'; // 기존/신규 유틸

type Item = {
  brand: string;
  code: string;
  family_slug: string | null;
  family_label: string | null;
  score: number;                // 0~1, nearest면 1 - dist
  datasheet_url?: string | null;
  thumb_url?: string | null;    // GCS 서명 URL or 프록시 URL
  box?: [number, number, number, number] | null; // [x,y,w,h] (이미지 좌표)
};

export async function analyzePhoto(req, res) {
  const { gcsUri, topK = 5, mode = 'detect-multi' } = req.body;

  // 1) (선택) 박스 검출
  const boxes = await getBoxes(gcsUri, mode); // [{id, xywh:[x,y,w,h]}...]

  // 2) 각 박스(or 전체 이미지)에 대해 nearest/guess 얻기
  const raw = await getNearestFromEmbedding(gcsUri, boxes, topK); 
  // 기대 형태:
  // { boxes:[{id, xywh}], nearest:[{brand,code,family_slug,dist,datasheet_uri,box_id?...}], guess:{brand,code,family_slug,confidence} }

  // 3) family_label 매핑
  const famMap = await loadFamilyMap(); // { relay_power: "Power Relay", ... }

  // 4) nearest → items
  let items: Item[] = [];
  if (Array.isArray(raw.nearest) && raw.nearest.length) {
    items = raw.nearest.slice(0, topK).map((r, i) => ({
      brand: r.brand ?? raw.guess?.brand ?? 'Unknown',
      code:  r.code  ?? raw.guess?.code  ?? `GUESS_${i+1}`,
      family_slug: r.family_slug ?? raw.guess?.family_slug ?? null,
      family_label: famMap[r.family_slug] ?? famMap[raw.guess?.family_slug] ?? null,
      score: typeof r.dist === 'number' ? Math.max(0, Math.min(1, 1 - r.dist)) : (raw.guess?.confidence ?? 0.5),
      datasheet_url: r.datasheet_uri ?? null,
      box: r.box_id ? (raw.boxes?.find(b => b.id === r.box_id)?.xywh ?? null) : null
    }));
  } else if (raw.guess) {
    items = [{
      brand: raw.guess.brand ?? 'Unknown',
      code:  raw.guess.code  ?? 'Unknown',
      family_slug: raw.guess.family_slug ?? null,
      family_label: famMap[raw.guess.family_slug] ?? null,
      score: raw.guess.confidence ?? 0.5,
      box: raw.boxes?.[0]?.xywh ?? null,
      datasheet_url: null
    }];
  }

  // 5) 각 item에 썸네일(공식 이미지) 붙이기: image_index 조회
  for (const it of items) {
    if (it.brand && it.code) {
      const thumb = await findOfficialThumb(it.brand, it.code); // 아래 함수
      it.thumb_url = thumb?.url ?? null;
    }
  }

  res.json({
    mode,
    boxes: raw.boxes ?? [],
    items
  });
}

async function loadFamilyMap(): Promise<Record<string,string>> {
  const { rows } = await pool.query(
    `SELECT family_slug, display_name FROM public.component_registry`
  );
  const m = {};
  rows.forEach(r => m[r.family_slug] = r.display_name);
  return m;
}

async function findOfficialThumb(brand: string, code: string) {
  const { rows } = await pool.query(
    `SELECT gcs_uri 
       FROM public.image_index 
      WHERE brand_norm = lower($1) AND code_norm = lower($2)
      ORDER BY created_at DESC 
      LIMIT 1`,
    [brand, code]
  );
  if (!rows.length) return null;

  // 서명 URL을 만들어 주거나, 워커 프록시 URL로 감싸서 프론트에 안전하게 전달
  const url = await signGcsUrl(rows[0].gcs_uri); 
  return { url };
}
