// src/utils/blueprint.ts
import { Pool } from "pg";

type BlueprintRow = {
  specs_table: string;
  fields_json: Record<string, string>;
};

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
// node-postgres 공식 Pool/parameterized query 권장 패턴입니다. :contentReference[oaicite:9]{index=9}

const TTL = Number(process.env.BLUEPRINT_CACHE_TTL_MS ?? 60_000);
const cache = new Map<string, { v: BlueprintRow; exp: number }>();
const pending = new Map<string, Promise<BlueprintRow>>(); // 동시 미스 중복 방지

export async function getBlueprint(family: string): Promise<BlueprintRow> {
  const now = Date.now();
  const hit = cache.get(family);
  if (hit && hit.exp > now) return hit.v;

  if (pending.has(family)) return pending.get(family)!;

  const p = (async () => {
    // 1) DB 조회 (단일 SoT)
    const { rows } = await pool.query<BlueprintRow>(
      `SELECT r.specs_table, b.fields_json
         FROM public.component_registry r
         JOIN public.component_spec_blueprint b
           ON b.family_slug = r.family_slug
        WHERE r.family_slug = $1`,
      [family]
    );
    let row = rows[0];

    // 2) 없으면 "최초 1회" seed → 다시 조회
    if (!row) {
      await seedFamilyIfMissing(family);
      const { rows: r2 } = await pool.query<BlueprintRow>(
        `SELECT r.specs_table, b.fields_json
           FROM public.component_registry r
           JOIN public.component_spec_blueprint b
             ON b.family_slug = r.family_slug
          WHERE r.family_slug = $1`,
        [family]
      );
      row = r2[0];
      if (!row) throw new Error(`family "${family}" not found after seeding`);
    }

    // 3) 캐시 저장
    cache.set(family, { v: row, exp: now + TTL });
    return row;
  })();

  pending.set(family, p);
  try {
    return await p;
  } finally {
    pending.delete(family);
  }
}

// 코드 내 기본 JSON은 "seed 용"으로만 존재. 조회 경로에서는 절대 사용 금지.
const DEFAULT_BLUEPRINTS: Record<
  string,
  { display_name: string; specs_table: string; fields_json: Record<string, string>; prompt_template?: string }
> = {
  // 예시) 필요 시 최소한으로 유지. (이미 DB에 많음)
  // resistor_chip: {
  //   display_name: "Chip Resistor",
  //   specs_table: "resistor_chip_specs",
  //   fields_json: { resistance_ohm: "numeric", power_w: "numeric", case_code: "text" }
  // }
};

async function seedFamilyIfMissing(family: string) {
  const seed = DEFAULT_BLUEPRINTS[family];
  if (!seed) return; // seed 없으면 아무 것도 안 함

  // 트랜잭션으로 "최초 1회"만 삽입 시도
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // UNIQUE/PK 없어도 동작하는 안전한 형태(레이스 가능성 낮음)
    await client.query(
      `INSERT INTO public.component_registry (family_slug, display_name, specs_table)
       SELECT $1, $2, $3
        WHERE NOT EXISTS (SELECT 1 FROM public.component_registry WHERE family_slug = $1)`,
      [family, seed.display_name, seed.specs_table]
    );

    await client.query(
      `INSERT INTO public.component_spec_blueprint (family_slug, fields_json, prompt_template, version)
       SELECT $1, $2::jsonb, $3, 1
        WHERE NOT EXISTS (SELECT 1 FROM public.component_spec_blueprint WHERE family_slug = $1)`,
      [family, JSON.stringify(seed.fields_json), seed.prompt_template ?? null]
    );

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}
