const { batchProcess } = require('./docai_client.cjs');
const { extractRelayFields } = require('./docai_parse.cjs');
console.log('OK:', typeof batchProcess, typeof extractRelayFields);
