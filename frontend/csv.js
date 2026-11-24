/* Shared CSV parser for the browser UI and its Node.js regression tests. */
(function (root, factory) {
  const parseCSV = factory();
  if (typeof module === 'object' && module.exports) {
    module.exports = { parseCSV };
  }
  if (root) root.parseCSV = parseCSV;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  return function parseCSV(text) {
    if (!text || typeof text !== 'string') return [];
    if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);

    const rows = [];
    let row = [];
    let field = '';
    let quoted = false;

    for (let i = 0; i < text.length; i += 1) {
      const char = text[i];
      const next = text[i + 1];
      if (quoted) {
        if (char === '"' && next === '"') {
          field += '"';
          i += 1;
        } else if (char === '"') {
          quoted = false;
        } else {
          field += char;
        }
      } else if (char === '"') {
        quoted = true;
      } else if (char === ',') {
        row.push(field);
        field = '';
      } else if (char === '\r' || char === '\n') {
        row.push(field);
        field = '';
        if (row.some(value => value.trim() !== '')) rows.push(row);
        row = [];
        if (char === '\r' && next === '\n') i += 1;
      } else {
        field += char;
      }
    }

    if (field !== '' || row.length > 0) {
      row.push(field);
      if (row.some(value => value.trim() !== '')) rows.push(row);
    }
    if (rows.length === 0) return [];

    const firstRowHasHeader = rows[0].some(cell =>
      /^(sku|raw_title|raw_description)$/i.test(cell.trim())
    );
    const dataRows = rows.slice(firstRowHasHeader ? 1 : 0);

    return dataRows
      .filter(columns => columns.some(value => value.trim() !== ''))
      .map(columns => ({
        sku: (columns[0] || '').trim(),
        // Preserve title text; the backend owns normalization.
        raw_title: columns[1] === undefined ? '' : columns[1],
        raw_description:
          columns[2] === undefined || columns[2] === '' ? null : columns[2],
      }));
  };
});
