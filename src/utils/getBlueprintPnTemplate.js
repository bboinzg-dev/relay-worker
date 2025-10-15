// @ts-check
/// <reference path="../types/blueprint.d.ts" />

'use strict';

/**
 * @param {import('../types/blueprint').Blueprint} blueprint
 * @param {import('../types/blueprint').Spec=} spec
 * @returns {string|null}
 */
function getBlueprintPnTemplate(blueprint, spec) {
  const bp = blueprint && typeof blueprint === 'object'
    ? blueprint
    : /** @type {import('../types/blueprint').Blueprint} */ ({});

  if (typeof bp.pn_template === 'string' && bp.pn_template.trim()) {
    return bp.pn_template.trim();
  }

  const codeRules = bp.code_rules;
  if (Array.isArray(codeRules)) {
    for (const rule of codeRules) {
      if (rule && typeof rule === 'object' && typeof rule.pn_template === 'string') {
        const template = rule.pn_template.trim();
        if (template) return template;
      }
    }
  } else if (codeRules && typeof codeRules === 'object' && typeof codeRules.pn_template === 'string') {
    const template = codeRules.pn_template.trim();
    if (template) return template;
  }

  const ingestOptions = /** @type {import('../types/blueprint').IngestOptions | null | undefined} */ (
    bp.ingestOptions ?? bp.ingest_options
  );
  if (ingestOptions && typeof ingestOptions === 'object' && typeof ingestOptions.pn_template === 'string') {
    const template = ingestOptions.pn_template.trim();
    if (template) return template;
  }

  if (spec && typeof spec === 'object' && typeof spec._pn_template === 'string') {
    const template = spec._pn_template.trim();
    if (template) return template;
  }

  return null;
}

module.exports = {
  getBlueprintPnTemplate,
};