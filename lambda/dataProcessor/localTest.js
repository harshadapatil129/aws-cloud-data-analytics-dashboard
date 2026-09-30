/**
 * Local Test Runner for AWS Lambda DataProcessor
 * Run: node lambda/dataProcessor/localTest.js
 * 
 * Tests the complete processing pipeline locally:
 * 1. Base64 payload decoding
 * 2. Field validation
 * 3. Data transformation & timestamping
 * 4. Error collection for dead-letter processing
 */

const fs = require('fs');
const path = require('path');
const { handler } = require('./index');

async function runLocalTest() {
  console.log('===========================================================');
  console.log('🧪 AWS Lambda: DataProcessor Local Test Simulation');
  console.log('===========================================================\n');

  const eventFilePath = path.join(__dirname, 'test-event.json');
  if (!fs.existsSync(eventFilePath)) {
    console.error('Error: test-event.json file not found!');
    process.exit(1);
  }

  const rawEvent = JSON.parse(fs.readFileSync(eventFilePath, 'utf-8'));
  console.log(`Loaded ${rawEvent.Records.length} mock Kinesis stream records.\n`);

  try {
    const response = await handler(rawEvent, {});

    console.log('\n--- 📊 Lambda Execution Results ---');
    console.log(`Status Code      : ${response.statusCode}`);
    console.log(`Total Records    : ${response.summary.totalRecords}`);
    console.log(`Valid Records    : ${response.summary.validRecords}`);
    console.log(`Rejected Records : ${response.summary.rejectedRecords}`);
    console.log(`Execution Time   : ${response.summary.totalDurationMs} ms`);

    console.log('\n--- 🔍 Transformed Valid Records (Ready for Redshift) ---');
    console.log(JSON.stringify(response.transformedSample, null, 2));

    console.log('\n✅ Local Lambda test completed successfully!');
    console.log('===========================================================');
  } catch (error) {
    console.error('\n❌ Lambda execution failed with error:', error);
  }
}

runLocalTest();
