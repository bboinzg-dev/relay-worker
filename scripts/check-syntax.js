#!/usr/bin/env node
'use strict';

const { readdirSync } = require('node:fs');
const { join } = require('node:path');
const { spawnSync } = require('node:child_process');

const root = process.cwd();
const files = readdirSync(root)
  .filter((name) => /^server.*\.js$/.test(name) && !name.endsWith('.bak'))
  .sort((a, b) => a.localeCompare(b));

if (!files.includes('server.js')) {
  files.unshift('server.js');
}

if (files.length === 0) {
  console.log('[syntax-check] no server*.js files found.');
  process.exit(0);
}

let failed = false;
for (const file of files) {
  const abs = join(root, file);
  const result = spawnSync(process.execPath, ['--check', abs], { stdio: 'inherit' });
  if (result.status !== 0) {
    console.error(`[syntax-check] syntax error detected in ${file}`);
    failed = true;
  }
}

if (failed) {
  process.exit(1);
}

console.log(`[syntax-check] ${files.length} files passed syntax check.`);