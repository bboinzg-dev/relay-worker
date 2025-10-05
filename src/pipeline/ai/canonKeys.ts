import { VertexAI } from "@google-cloud/vertexai";

type CanonicalResult = {
  canonical: string;
  action: "map" | "new";
  conf: number;
};

type CanonicalMap = Record<string, CanonicalResult>;

const MODEL = process.env.GEMINI_MODEL_CLASSIFY || "gemini-1.5-pro-002";
const MIN_CONF = Number(process.env.AUTO_CANON_MIN_CONF || "0.66");

export async function aiCanonicalizeKeys(
  family: string,
  keys: string[],
  knownKeys: string[]
): Promise<{ map: CanonicalMap; newKeys: string[] }> {
  const vertex = new VertexAI({
    project: process.env.GCP_PROJECT_ID,
    location: process.env.VERTEX_LOCATION || "asia-northeast3",
  });
  const model = vertex.getGenerativeModel({
    model: MODEL,
    generationConfig: { temperature: 0.1, maxOutputTokens: 1024 }
  });

  const prompt = `
당신은 전자부품 데이터시트 스펙 키 표준화기입니다.
family="${family}".
KNOWN_KEYS 목록 중에서 가장 가까운 키를 고르거나, 없으면 "new"로 표시하세요.
반드시 아래 JSON 형식만 출력하세요.

입력 키 배열: ${JSON.stringify(keys)}
KNOWN_KEYS: ${JSON.stringify(knownKeys)}

형식:
{
 "input_key": {"canonical": "<known_or_new_name>", "action": "map"|"new", "conf": 0.0~1.0}
}

규칙:
- "map"일 때 canonical은 KNOWN_KEYS 중 하나여야 함.
- 애매하면 "new"로.
- conf는 당신의 확신도(0~1).
`;

  const { response } = await model.generateContent(prompt);
  const text = typeof response?.text === "function"
    ? response.text()
    : (response?.candidates?.[0]?.content?.parts?.map((p: any) => p?.text || "").join("") || "{}");
  let obj: any = {};
  try { obj = JSON.parse(text); } catch { obj = {}; }

  const out: CanonicalMap = {};
  const newKeys: string[] = [];
  for (const k of keys) {
    const rec = obj[k] || {};
    let canonical = String(rec.canonical || "").trim();
    let action: "map" | "new" = (rec.action === "map" && knownKeys.includes(canonical)) ? "map" : "new";
    let conf = Number(rec.conf || 0);
    if (action === "map" && conf < MIN_CONF) action = "new";
    out[k] = { canonical: canonical || k, action, conf };
    if (action === "new") newKeys.push(k);
  }
  return { map: out, newKeys };
}