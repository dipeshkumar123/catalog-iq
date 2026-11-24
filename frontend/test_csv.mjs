import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const { parseCSV } = require('./csv.js');

const cases = [
  ['basic header', 'sku,raw_title,raw_description\nA1,Amul Butter,pck of 2', row => {
    assert.deepEqual(row, { sku: 'A1', raw_title: 'Amul Butter', raw_description: 'pck of 2' });
  }],
  ['quoted comma', 'sku,raw_title,raw_description\nA1,"Butter, salted",500g', row => {
    assert.equal(row.raw_title, 'Butter, salted');
  }],
  ['escaped quotes', 'sku,raw_title,raw_description\nA1,"Say ""hi""",desc', row => {
    assert.equal(row.raw_title, 'Say "hi"');
  }],
  ['CRLF', 'sku,raw_title,raw_description\r\nA1,Test,desc\r\n', row => {
    assert.equal(row.raw_title, 'Test');
  }],
  ['BOM', '\uFEFFsku,raw_title,raw_description\nA1,Test,desc', row => {
    assert.equal(row.raw_title, 'Test');
  }],
  ['header omitted', 'A1,Test,desc', row => {
    assert.deepEqual(row, { sku: 'A1', raw_title: 'Test', raw_description: 'desc' });
  }],
  ['missing description', 'sku,raw_title,raw_description\nA1,Test,', row => {
    assert.ok(row.raw_description === '' || row.raw_description === null);
  }],
  ['title whitespace preserved', 'sku,raw_title\nA1,  Amul   Butter  ', row => {
    assert.equal(row.raw_title, '  Amul   Butter  ');
  }],
];

for (const [name, input, check] of cases) {
  const rows = parseCSV(input);
  assert.equal(rows.length, 1, `${name}: expected one product row`);
  check(rows[0]);
  process.stdout.write(`PASS ${name}\n`);
}
