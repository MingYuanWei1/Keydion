/* Browser/CommonJS helper for lifecycle error payloads returned by upload XHR. */
(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.KeydionUploadErrors = api;
})(typeof window !== 'undefined' ? window : globalThis, function () {
  'use strict';

  function usableString(value) {
    return typeof value === 'string' && value.trim() ? value.trim() : '';
  }

  function formatUploadError(error, fallback) {
    const safeFallback = usableString(fallback) || 'Upload failed. Please try again.';
    const legacy = usableString(error);
    if (legacy) return legacy;
    if (!error || typeof error !== 'object' || Array.isArray(error)) {
      return safeFallback;
    }

    const message = usableString(error.message);
    if (!message) return safeFallback;
    const fields = error.field_errors;
    if (!fields || typeof fields !== 'object' || Array.isArray(fields)) {
      return message;
    }
    const details = Object.entries(fields)
      .map(([field, value]) => [usableString(field), usableString(value)])
      .filter(([field, value]) => field && value)
      .map(([field, value]) => `${field}: ${value}`);
    return details.length ? `${message} (${details.join('; ')})` : message;
  }

  return { formatUploadError };
});
