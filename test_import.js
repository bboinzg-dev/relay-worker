const { batchProcess } = require('./docai_client');
const { extractRelayFields } = require('./docai_parse');
console.log('OK:', typeof batchProcess, typeof extractRelayFields);
