# Security

## Authentication and requests

- Gate protected routes with `require_login(level=...)`. Role levels are 1 Reader, 2 Contributor, and 3 Curator; use the existing authorization conversion instead of comparing raw stored role strings.
- Global CSRF protection stays enabled. Every POST form includes `{{ csrf_token() }}`; every mutating `fetch` sends `X-CSRFToken` from the page's CSRF meta tag.
- Logout remains POST-only.
- Pass user-controlled redirects through `_safe_redirect_path()`. Preserve the OAuth flow that consumes and verifies the saved `state` before login completes.

Session cookies remain `SameSite=Lax`, `HttpOnly`, and `Secure` unless `PAPERQUERY_COOKIE_SECURE` disables `Secure` for local HTTP. Security headers stay centralized in `create_app()`; the current frame policy is `SAMEORIGIN`, and CSP remains Report-Only rather than enforced.

## Stored content and files

- Sanitize guide and news rich text with the existing server-side sanitizers before persistence. Render `|safe` only after that boundary.
- Resolve user-controlled storage names with `services.papers.resolve_contained()`. Use `must_exist=True` for reads, serving, and deletion, and use the publishing/storage services for Paper mutations instead of direct path operations.
