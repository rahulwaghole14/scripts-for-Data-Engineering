const uuidv5 = require('uuid').v5;

// Namespace and name
const namespace = 'ee7c9503-34e9-41b5-8555-ca9d738869d2';  // replace this with your actual namespace UUID
const email = 'hexa__sean.godfrey@hexa.co.nz';

// Generate the UUIDv5
const uuid = uuidv5(email, namespace);

console.log(`Generated UUIDv5: ${uuid}`);
// Generated UUIDv5: 2b0a02ff-4430-59a3-8beb-2056d21d5690
