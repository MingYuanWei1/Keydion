# Change Password — Design Spec

**Date:** 2026-05-23
**Status:** Approved (pending implementation plan)

## 1. Summary

Connect the new editorial `templates/change_password.html` (currently
uncommitted in the user's main checkout) to the existing
`/dashboard/account/change-password` route, with three behavior changes to the
server-side handler:

1. **Verify the current password** before allowing a change (when the user
   already has one). The new template posts a `current_password` field that
   the current route silently ignores.
2. **Stay on the same page after success**, displaying a success flash inside
   the form card — instead of redirecting to the dashboard overview.
3. **Tighten validation** to match what the new template's live checklist
   promises the user: letters-and-digits required; new password must differ
   from current. Length stays at `>= 6` (template UI copy is adjusted down to
   match).

No new routes, no new templates, no JS changes outside the inline `<script>`
already inside the new template.

## 2. Files touched

| File | Change |
|------|--------|
| `app.py` (`change_password`, lines 908–948) | Server-side validation rewrite |
| `templates/change_password.html` | New body (carried over from the user's working copy) + copy/JS edits: "8" → "6" |
| `translations/en/LC_MESSAGES/messages.po` | New strings |
| `translations/zh/LC_MESSAGES/messages.po` | New strings (translated) |
| `translations/*/LC_MESSAGES/messages.mo` | Recompiled |
| `tests/test_change_password_contract.py` (new) | Contract test for the route |

## 3. Server route (`app.py:908`)

### POST flow

1. `current_password = request.form.get("current_password", "")`
   `new_password     = request.form.get("new_password", "").strip()`
   `confirm_password = request.form.get("confirm_password", "").strip()`

   `current_password` is read without `.strip()` — leading/trailing spaces in a
   password are user-chosen and must be compared verbatim against the stored
   hash.

2. **Compute `has_password`** (same logic the GET branch uses, lifted above the
   POST branch so both can share it):
   - Local users: `has_password = True` (LocalUser rows always have a hash).
   - MS users: `ms_record = get_ms_user(ms_id); has_password = bool(ms_record and ms_record.get("password"))`.

3. **Verify current password (only when `has_password` is True):**
   - Fetch stored hash:
     - Local: `record = get_local_user(user["username"])` → `record["password"]`
     - MS:    reuse `ms_record` from step 2 → `ms_record["password"]`
   - If the record is missing or `not verify_password(current_password, record["password"])`:
     `flash(_("Current password is incorrect."), "danger")` →
     `redirect(url_for("change_password"))`.

   Uses the existing `verify_password` helper at `app.py:3465` — the same one
   `authenticate` (app.py:3115) and the MS-login email fallback (app.py:765)
   already use. No new verify helper needed.

4. **Validate `new_password`** (in this order, first failure short-circuits with
   a flash + redirect to `change_password`):

   | Check | Flash message (category) |
   |-------|--------------------------|
   | non-empty | `_("Please enter a new password.")` (`warning`) — existing |
   | matches `confirm_password` | `_("Passwords do not match.")` (`warning`) — existing |
   | `len(new_password) >= 6` | `_("Password must be at least 6 characters.")` (`warning`) — existing |
   | contains a letter AND a digit | `_("Password must contain both letters and numbers.")` (`warning`) — NEW |
   | if `has_password`: `new_password != current_password` | `_("New password must be different from your current password.")` (`warning`) — NEW |

   Letter/digit check:
   `any(c.isalpha() for c in new_password) and any(c.isdigit() for c in new_password)`.

   Symbol requirement (the third item in the template's checklist) is **NOT**
   enforced server-side — the template already marks it `is-soft` and labels
   it "(recommended)".

5. **Persist**:
   - MS user: `update_ms_user_password(ms_id, new_password)`
   - Local user: `update_local_user_password(user["username"], new_password)`

6. **Respond**:
   - Success: `flash(_("Password updated successfully."), "success")` then
     `redirect(url_for("change_password"))` — **changed from `dashboard`**.
   - Failure: `flash(_("Unable to update password."), "danger")` then
     `redirect(url_for("change_password"))` (same URL it already used on
     failure).

### GET flow

Unchanged. Renders `change_password.html` with `user`, `has_password`.

### Legacy redirect

`change_password_legacy` at `app.py:950` (the `/account/change-password` →
`/dashboard/account/change-password` 301) is untouched.

## 4. Template adjustments (`templates/change_password.html`)

The new template body in the user's uncommitted working copy is carried over
into this work as-is, with two small honesty edits against the server:

1. Requirements-list copy: `_('At least 8 characters')` → `_('At least 6 characters')`.
2. The inline JS `setReq('length', pw.length >= 8)` → `setReq('length', pw.length >= 6)`.
3. The placeholder `placeholder="{{ _('At least 8 characters') }}"` on
   `#new_password` → `_('At least 6 characters')`.

`<input minlength="6">` is already correct on both `new_password` and
`confirm_password` — no change.

Flash rendering at the top of `<form class="cp-card">` already iterates
`get_flashed_messages(with_categories=True)` and styles the `success`,
`danger`, and `warning` categories used by the route — no change.

## 5. Dashboard-shell integration

No code changes. The shell at `static/js/dashboard.js:127` already intercepts
any form submitted inside `#dashboardMain` and POSTs it with the
`X-Partial-Content: 1` header, then follows the response (including redirects
that stay under `/dashboard/*`) and swaps the resulting HTML into the panel.

With the server now redirecting to `change_password` (a URL under
`/dashboard/*`) instead of `dashboard`, the partial loader:

- Follows the 302 to `/dashboard/account/change-password`.
- Receives the page rendered via `_bare.html` (because the partial header is
  present), which produces just the inner `{% block panel %}` content.
- Swaps that HTML into `#dashboardMain`.
- Runs the inline `<script>` (the script-replay loop at dashboard.js:92
  re-creates `<script>` tags so they execute).
- `pushState`s the resolved URL (same as the current URL — a no-op).

The user sees the success flash inside the same form card, without the page
appearing to navigate.

## 6. i18n

### Strings to add

| msgid | en | zh |
|-------|----|----|
| `Current password is incorrect.` | (same) | `当前密码不正确。` |
| `New password must be different from your current password.` | (same) | `新密码必须与当前密码不同。` |
| `Password must contain both letters and numbers.` | (same) | `密码必须包含字母和数字。` |

Plus the template strings introduced in the user's new template body:
`Current password`, `Enter your current password`, `Confirm new password`,
`Show password`, `Hide password`, `Passwords match`, `Passwords don't match`,
`Too short`, `Weak`, `Fair`, `Good`, `Strong`, `At least 6 characters`,
`Letters and numbers`, `One symbol (recommended)`,
`Different from your current password`, `Password requirements`, `Account`,
`Change password`, `Set a password`, `Update password`, `Set password`,
`Re-enter your new password`,
`Update the password used to sign in to Keydion with your email or username.`,
`Create a password so you can sign in with email and password in the future.`.

The existing reused strings (`Please enter a new password.`,
`Passwords do not match.`, `Password must be at least 6 characters.`,
`Password updated successfully.`, `Unable to update password.`) already have
translations and need no work.

### Process

1. Edit `translations/en/LC_MESSAGES/messages.po` — add the new entries
   (`msgstr` typically matches `msgid` in en).
2. Edit `translations/zh/LC_MESSAGES/messages.po` — add new entries with
   translated `msgstr`.
3. Run `python tools/compile_translations.py` to produce updated `.mo` files.

Both `.po` and `.mo` files are committed.

## 7. Testing

New file: `tests/test_change_password_contract.py`. Follows the existing
contract-test style — uses `ast` to parse `app.py` and assert structural
invariants on the `change_password` view function (lookup by name in
`create_app`'s body).

Assertions:

1. The function's source text contains `request.form.get("current_password"`
   (or equivalent — assert presence of the literal string `current_password`
   in form-read positions).
2. The function calls `verify_password(` somewhere within its body (textual
   check), AND references `current_password`.
3. The function's success path redirects to `url_for("change_password")`.
   Walk the AST for `Call(func=Name("redirect"))` nodes whose first argument
   is `Call(func=Name("url_for"), args=[Str("change_password")])` — assert at
   least one exists. AND assert no AST node calls `url_for("dashboard")`
   inside this function (catches accidental regression to the old destination).
4. Renders the template with `has_password` in its context — preserved from
   the existing route (assert the keyword `has_password` is passed to
   `render_template`).

Also: render `templates/change_password.html` with a stub Jinja env (mirroring
the existing template-render contract tests in `tests/`) and verify it
produces a `<form>` containing inputs with `name="current_password"`,
`name="new_password"`, and `name="confirm_password"` when `has_password=True`,
and omits the `current_password` input when `has_password=False`.

Run: `python -m unittest tests/test_change_password_contract.py -v`.

## 8. Out of scope

- Rate-limiting failed `current_password` attempts. The codebase has no
  existing rate-limit infrastructure; adding it here would be net-new and
  unrelated.
- Forcing logout-of-other-sessions after a password change. `SessionModel`
  exists but there is no precedent for invalidating sibling sessions on
  password change; out of scope.
- Email notification on password change. Out of scope.
- Symbol-requirement enforcement. Template marks it as "recommended"; server
  follows the template.
- Touching `change_password_legacy` (the `/account/change-password` 301
  redirect at `app.py:950`).
- Any restyle of the new template's editorial design. The template arrived
  fully styled; this work only adjusts the "8" → "6" honesty edits.

## 9. Implementation note: template provenance

The new template body lives only in the user's uncommitted working copy at
`/Users/mingyuanw/Desktop/Project/Keydion/templates/change_password.html` —
not in any branch. The implementation plan must include a step to copy that
working-copy file into the worktree (e.g.,
`cp ../../../templates/change_password.html templates/change_password.html`
from the worktree root) before applying the "8" → "6" edits.
