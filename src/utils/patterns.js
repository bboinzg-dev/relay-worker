'use strict';

const PN_BODY_CHARS = 'A-Za-z0-9\\-_.\\/()#';
const PN_CANDIDATE_SOURCE = `[A-Za-z][A-Za-z0-9](?:[${PN_BODY_CHARS}]{1,35})`;
const PN_FULL_SOURCE = `[A-Za-z0-9][${PN_BODY_CHARS}]{3,63}[A-Za-z0-9)#]`;

const PN_RE = new RegExp(`\\b${PN_FULL_SOURCE}\\b`, 'i');
const PN_CANDIDATE_RE = new RegExp(PN_CANDIDATE_SOURCE, 'g');

module.exports = {
  PN_RE,
  PN_CANDIDATE_RE,
  PN_BODY_CHARS,
  PN_CANDIDATE_SOURCE,
};