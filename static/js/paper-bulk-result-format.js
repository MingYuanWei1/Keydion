/* Browser/CommonJS helper for safe bulk Paper delete outcome messages. */
(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.KeydionPaperBulkResults = api;
})(typeof window !== 'undefined' ? window : globalThis, function () {
  'use strict';

  function safeCount(value) {
    return Number.isSafeInteger(value) && value >= 0 ? value : 0;
  }

  function outcomeCount(result, field, legacyCountField) {
    if (!result || typeof result !== 'object' || Array.isArray(result)) return 0;
    if (Array.isArray(result[field])) return result[field].length;
    return legacyCountField ? safeCount(result[legacyCountField]) : 0;
  }

  function papers(count) {
    return count === 1 ? 'paper' : 'papers';
  }

  const defaults = {
    deleted: count => `Deleted ${count} ${papers(count)} successfully`,
    deleting: count => `Deletion in progress for ${count} ${papers(count)}`,
    stale: count => `${count} ${papers(count)} changed while deleting; reload and try again`,
    notFound: count => `${count} ${papers(count)} ${count === 1 ? 'was' : 'were'} not found`,
  };

  function formatter(labels, name) {
    return labels && typeof labels[name] === 'function'
      ? labels[name]
      : defaults[name];
  }

  function formatDeleteResult(result, labels) {
    const groups = [
      ['deleted', outcomeCount(result, 'deleted', 'count')],
      ['deleting', outcomeCount(result, 'deleting', 'deleting_count')],
      ['stale', outcomeCount(result, 'stale')],
      ['notFound', outcomeCount(result, 'not_found')],
    ];
    return groups
      .filter(([_name, count]) => count > 0)
      .map(([name, count]) => formatter(labels, name)(count))
      .filter(message => typeof message === 'string' && message)
      .join(' · ');
  }

  return { formatDeleteResult };
});
