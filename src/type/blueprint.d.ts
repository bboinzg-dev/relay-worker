// lightweight runtime-agnostic types for blueprint/spec

export type FieldType = 'string' | 'boolean' | 'numeric' | 'number' | 'text';

export interface FieldDef {
  type: FieldType;
  // optional extractor hints or metadata
  [k: string]: unknown;
}

export type FieldsJson = Record<string, FieldType | FieldDef>;

export interface CodeRule {
  pn_template?: string;
  [k: string]: unknown;
}

export interface IngestOptions {
  variant_keys?: string[];
  fast_keys?: string[];
  page_hints?: string[];
  pn_template?: string;
  [k: string]: unknown;
}

export interface Blueprint {
  family_slug?: string;
  fields_json?: FieldsJson;
  prompt_template?: string;
  version?: number | string;

  // preferred top-level
  pn_template?: string | null;

  // observed in DB/loader: can be array or single object
  code_rules?: CodeRule[] | CodeRule | null;

  // both spellings exist in codebase/DB
  ingestOptions?: IngestOptions | null;
  ingest_options?: IngestOptions | null;
}

export interface Spec {
  _pn_template?: string | null;
  [k: string]: unknown;
}

// dim keys (legacy + normalized) — use to avoid typos
export const DIM_KEYS: readonly [
  'length_mm','width_mm','height_mm','dim_l_mm','dim_w_mm','dim_h_mm'
];

export type DimensionKey = typeof DIM_KEYS[number];

// helper signature (impl in JS/TS below)
export function getBlueprintPnTemplate(
  blueprint: Blueprint,
  spec?: Spec | null
): string | null;