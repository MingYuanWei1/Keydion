# Change Password Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the new editorial `templates/change_password.html` to its server route so password changes verify the current password, enforce letters-and-digits, reject reusing the same password, and keep the user on the same page with a success flash.

**Architecture:** Server-side rewrite of the existing view function `change_password` at `app.py:908`. Validation rules tightened to match the template's live checklist. Success path redirects back to the same URL (PRG) so the dashboard partial loader (`static/js/dashboard.js:127`) re-renders the form in place with the success flash visible. Inline JS in the template is unchanged except for two "8" → "6" copy edits to keep the live requirements list honest with the server.

**Tech Stack:** Flask, SQLAlchemy, Flask-Babel, Jinja2, Python `unittest` + `ast` for contract tests.

**Spec:** `docs/superpowers/specs/2026-05-23-change-password-design.md`.

---

## File structure

| File | Role | Created in task |
|------|------|-----------------|
| `templates/change_password.html` | Form UI; copy + JS edits only ("8" → "6") | Task 1 |
| `app.py` (view `change_password`, ~lines 908–948) | POST handler rewrite | Tasks 2, 3, 4 |
| `tests/test_change_password_contract.py` | Contract tests (AST + Jinja render) | Tasks 2–5 |
| `translations/en/LC_MESSAGES/messages.po` | New msgids (msgstr = msgid) | Task 6 |
| `translations/zh/LC_MESSAGES/messages.po` | New msgids with Chinese msgstr | Task 6 |
| `translations/{en,zh}/LC_MESSAGES/messages.mo` | Recompiled binaries | Task 6 |

The view function and the route registration both stay in `app.py` — the codebase keeps all routes in that single file (see `CLAUDE.md`), and splitting one view out would break the convention.

---

## Task 1: Reconcile template requirement copy with server (length: 8 → 6)

The new template (committed in `4b9a6d5`) promises "At least 8 characters" in three places, but the server enforces (and will continue to enforce) `>= 6`. Bring the template down to match.

**Files:**
- Modify: `templates/change_password.html`

- [ ] **Step 1: Locate the three "8" references**

Run: `grep -n "8 characters\|>= 8\|At least 8" templates/change_password.html`

Expected output (line numbers approximate):

```
~256:          placeholder="{{ _('At least 8 characters') }}" required minlength="6"
~339:        <span>{{ _('At least 8 characters') }}</span>
~542:      setReq('length', pw.length >= 8);
```

- [ ] **Step 2: Edit the placeholder on `#new_password`**

In `templates/change_password.html`, change:

```
placeholder="{{ _('At least 8 characters') }}" required minlength="6"
```

to:

```
placeholder="{{ _('At least 6 characters') }}" required minlength="6"
```

- [ ] **Step 3: Edit the requirements-list label**

In `templates/change_password.html`, inside the `<li data-req="length">` block, change:

```
        <span>{{ _('At least 8 characters') }}</span>
```

to:

```
        <span>{{ _('At least 6 characters') }}</span>
```

- [ ] **Step 4: Edit the inline JS length check**

In `templates/change_password.html`, in the `refresh()` function, change:

```
      setReq('length', pw.length >= 8);
```

to:

```
      setReq('length', pw.length >= 6);
```

- [ ] **Step 5: Verify all three are gone**

Run: `grep -n "8 characters\|>= 8\|At least 8" templates/change_password.html`

Expected: no output (exit code 1).

Also: `grep -nc "At least 6" templates/change_password.html`

Expected: `3`

- [ ] **Step 6: Commit**

```bash
git add templates/change_password.html
git commit -m "fix(account): align change-password requirements copy with server (6 chars)"
```

---

## Task 2: Test + implement — success redirects to change_password (not dashboard)

The current view redirects to `dashboard` after both success and failure of the persist call. We change that single line so the user stays on the change-password page; the dashboard partial loader will re-render in place and show the success flash.

**Files:**
- Create: `tests/test_change_password_contract.py`
- Modify: `app.py` (function `change_password`, line ~940 — the final `return redirect(url_for("dashboard"))`)

- [ ] **Step 1: Write the failing test**

Create `tests/test_change_password_contract.py` with the following content:

```python
import ast
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ChangePasswordContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app_source = (ROOT / "app.py").read_text(encoding="utf-8")
        cls.template_source = (
            ROOT / "templates" / "change_password.html"
        ).read_text(encoding="utf-8")
        cls.app_tree = ast.parse(cls.app_source)
        cls.view = cls._find_function("change_password")
        cls.view_source = ast.get_source_segment(cls.app_source, cls.view)

    @classmethod
    def _find_function(cls, name):
        for node in ast.walk(cls.app_tree):
            if isinstance(node, ast.FunctionDef) and node.name == name:
                return node
        raise AssertionError(f"Could not find function {name}")

    # --- Task 2: redirect destination ---------------------------------

    def test_success_redirects_to_change_password_not_dashboard(self):
        url_for_calls = []
        for node in ast.walk(self.view):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "url_for"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                url_for_calls.append(node.args[0].value)

        self.assertIn(
            "change_password",
            url_for_calls,
            "change_password view should redirect back to itself after POST",
        )
        self.assertNotIn(
            "dashboard",
            url_for_calls,
            "change_password view should not redirect to the dashboard overview",
        )


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m unittest tests/test_change_password_contract.py -v`

Expected: `FAIL` on `test_success_redirects_to_change_password_not_dashboard` with a message like `"dashboard" should not appear in url_for calls`.

- [ ] **Step 3: Change the redirect destination**

Open `app.py` and find the `change_password` view at `app.py:908`. The function ends with (around lines 936–940):

```python
            if success:
                flash(_("Password updated successfully."), "success")
            else:
                flash(_("Unable to update password."), "danger")
            return redirect(url_for("dashboard"))
```

Change the final line to:

```python
            return redirect(url_for("change_password"))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m unittest tests/test_change_password_contract.py -v`

Expected: `OK` (1 test passed).

- [ ] **Step 5: Commit**

```bash
git add tests/test_change_password_contract.py app.py
git commit -m "feat(account): keep user on change-password page after submit"
```

---

## Task 3: Test + implement — verify the current password

Add server-side verification of the `current_password` field (only when `has_password` is True). Uses the existing `verify_password` helper at `app.py:3465` and the existing `get_local_user` / `get_ms_user` lookups.

**Files:**
- Modify: `tests/test_change_password_contract.py` (add tests)
- Modify: `app.py` (function `change_password`, POST branch)

- [ ] **Step 1: Add the failing tests**

Append the following methods to `ChangePasswordContractTest` in `tests/test_change_password_contract.py` (just before the `if __name__ == "__main__":` block):

```python
    # --- Task 3: current-password verification ------------------------

    def test_view_reads_current_password_from_form(self):
        self.assertRegex(
            self.view_source,
            r'request\.form\.get\(\s*["\']current_password["\']',
            "change_password view must read current_password from request.form",
        )

    def test_view_calls_verify_password_on_current_password(self):
        # verify_password(...) must appear inside the view body.
        verify_calls = [
            node for node in ast.walk(self.view)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "verify_password"
        ]
        self.assertTrue(
            verify_calls,
            "change_password view must call verify_password(...) to check the current password",
        )
        # And one of those calls must pass the current_password variable as
        # the first argument (heuristic: arg is a Name node).
        first_arg_names = [
            call.args[0].id
            for call in verify_calls
            if call.args and isinstance(call.args[0], ast.Name)
        ]
        self.assertIn(
            "current_password",
            first_arg_names,
            "verify_password must be called with current_password as its first argument",
        )
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `python -m unittest tests/test_change_password_contract.py -v`

Expected: 2 failures — `test_view_reads_current_password_from_form` and `test_view_calls_verify_password_on_current_password`. The redirect test from Task 2 still passes.

- [ ] **Step 3: Restructure the POST branch to verify current_password**

Open `app.py` and find the `change_password` view at `app.py:908`. The current POST branch starts at the `if request.method == "POST":` line. Also notice that `has_password` is currently computed only in the GET branch (after the POST returns).

Lift the `has_password` computation above the POST branch and add the current-password verification. Replace the existing block:

```python
        is_ms_user = not user.get("is_local", True)
        ms_id = user.get("ms_id") or user.get("username", "")

        if request.method == "POST":
            new_password = request.form.get("new_password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()
```

with:

```python
        is_ms_user = not user.get("is_local", True)
        ms_id = user.get("ms_id") or user.get("username", "")

        # Determine if the user already has a password set. MS-only users may
        # have arrived via Microsoft sign-in without ever setting a local
        # password — in that case current-password verification is skipped.
        ms_record = get_ms_user(ms_id) if is_ms_user else None
        has_password = True
        if is_ms_user:
            has_password = bool(ms_record and ms_record.get("password"))

        if request.method == "POST":
            current_password = request.form.get("current_password", "")
            new_password = request.form.get("new_password", "").strip()
            confirm_password = request.form.get("confirm_password", "").strip()

            if has_password:
                if is_ms_user:
                    stored_hash = (ms_record or {}).get("password", "")
                else:
                    local_record = get_local_user(user.get("username", "")) or {}
                    stored_hash = local_record.get("password", "")
                if not stored_hash or not verify_password(current_password, stored_hash):
                    flash(_("Current password is incorrect."), "danger")
                    return redirect(url_for("change_password"))
```

Then, **delete** the now-redundant `has_password` computation that lives between the POST branch and the `render_template` call (around lines 942–946 in the original file):

```python
        # Determine if the user already has a password set
        has_password = True
        if is_ms_user:
            ms_record = get_ms_user(ms_id)
            has_password = bool(ms_record and ms_record.get("password"))

```

The `render_template("change_password.html", user=user, has_password=has_password)` line at the bottom stays as-is and now uses the `has_password` computed at the top of the view.

- [ ] **Step 4: Run all tests to verify they pass**

Run: `python -m unittest tests/test_change_password_contract.py -v`

Expected: `OK` (3 tests pass).

- [ ] **Step 5: Commit**

```bash
git add tests/test_change_password_contract.py app.py
git commit -m "feat(account): verify current password before allowing change"
```

---

## Task 4: Test + implement — composition rule + different-from-current

Add two new validation rules to the POST branch:

1. New password must contain at least one letter AND one digit.
2. When `has_password`, the new password must differ from the current password.

**Files:**
- Modify: `tests/test_change_password_contract.py` (add tests)
- Modify: `app.py` (function `change_password`, POST branch — append new checks after the existing length check)

- [ ] **Step 1: Add the failing tests**

Append the following methods to `ChangePasswordContractTest` (just before `if __name__ == "__main__":`):

```python
    # --- Task 4: composition + different-from-current -----------------

    def test_view_enforces_letters_and_digits(self):
        # Look for the letters+digits check: any(c.isalpha()...) and any(c.isdigit()...)
        has_alpha = "c.isalpha()" in self.view_source
        has_digit = "c.isdigit()" in self.view_source
        self.assertTrue(
            has_alpha and has_digit,
            "change_password view must enforce a letters-and-digits rule "
            "(expected `any(c.isalpha() for c in ...)` and "
            "`any(c.isdigit() for c in ...)` in the function body)",
        )

    def test_view_rejects_unchanged_password(self):
        # The new password must differ from current. We check for a
        # comparison between new_password and current_password.
        comparisons = re.findall(
            r"new_password\s*(?:!=|==)\s*current_password|"
            r"current_password\s*(?:!=|==)\s*new_password",
            self.view_source,
        )
        self.assertTrue(
            comparisons,
            "change_password view must compare new_password against "
            "current_password to reject reuse",
        )
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `python -m unittest tests/test_change_password_contract.py -v`

Expected: 2 failures — `test_view_enforces_letters_and_digits` and `test_view_rejects_unchanged_password`. The other 3 tests still pass.

- [ ] **Step 3: Add the two new validation checks**

In `app.py`, inside the `change_password` view's POST branch, find the existing length check:

```python
            if len(new_password) < 6:
                flash(_("Password must be at least 6 characters."), "warning")
                return redirect(url_for("change_password"))
```

Immediately after that block, add:

```python
            has_alpha = any(c.isalpha() for c in new_password)
            has_digit = any(c.isdigit() for c in new_password)
            if not (has_alpha and has_digit):
                flash(_("Password must contain both letters and numbers."), "warning")
                return redirect(url_for("change_password"))

            if has_password and new_password == current_password:
                flash(
                    _("New password must be different from your current password."),
                    "warning",
                )
                return redirect(url_for("change_password"))
```

- [ ] **Step 4: Run all tests to verify they pass**

Run: `python -m unittest tests/test_change_password_contract.py -v`

Expected: `OK` (5 tests pass).

- [ ] **Step 5: Commit**

```bash
git add tests/test_change_password_contract.py app.py
git commit -m "feat(account): require letters+digits and a fresh password"
```

---

## Task 5: Template-render contract test (regression guard)

Add Jinja render assertions so future template edits can't silently drop the `current_password` input or break the `has_password=False` branch.

**Files:**
- Modify: `tests/test_change_password_contract.py` (add tests)

- [ ] **Step 1: Add the template-render tests**

Append the following methods to `ChangePasswordContractTest` (just before `if __name__ == "__main__":`):

```python
    # --- Task 5: template render contract ------------------------------

    @staticmethod
    def _render(has_password):
        from jinja2 import DictLoader, Environment

        env = Environment(
            loader=DictLoader({
                "_bare.html": "{% block panel %}{% endblock %}",
                "_dashboard_shell.html": "{% block panel %}{% endblock %}",
                "change_password.html": (
                    ROOT / "templates" / "change_password.html"
                ).read_text(encoding="utf-8"),
            }),
            autoescape=True,
            extensions=["jinja2.ext.i18n"],
        )
        env.install_null_translations(newstyle=True)
        env.globals["url_for"] = lambda name, **_: "/" + name.replace("_", "-")
        env.globals["get_flashed_messages"] = lambda **_: []
        return env.get_template("change_password.html").render(
            user={"username": "alice"},
            has_password=has_password,
            partial=False,
        )

    def test_template_has_current_password_input_when_has_password(self):
        html = self._render(has_password=True)
        self.assertRegex(
            html,
            r'<input[^>]+name="current_password"',
            "template must include a current_password input when has_password=True",
        )
        self.assertRegex(html, r'<input[^>]+name="new_password"')
        self.assertRegex(html, r'<input[^>]+name="confirm_password"')

    def test_template_omits_current_password_input_for_first_time_set(self):
        html = self._render(has_password=False)
        self.assertNotIn(
            'name="current_password"',
            html,
            "template must omit the current_password input when has_password=False",
        )
        self.assertRegex(html, r'<input[^>]+name="new_password"')
        self.assertRegex(html, r'<input[^>]+name="confirm_password"')
```

- [ ] **Step 2: Run the tests to verify they pass (regression guards, not red→green)**

The new template body (committed in `4b9a6d5`) already wraps the current-password field in `{% if has_password %}`, so these tests should pass on first run.

Run: `python -m unittest tests/test_change_password_contract.py -v`

Expected: `OK` (7 tests pass).

If a test fails: the template render is broken. Read the failure, fix the template, re-run. Do NOT skip the test.

- [ ] **Step 3: Commit**

```bash
git add tests/test_change_password_contract.py
git commit -m "test(account): guard change_password template current-password branch"
```

---

## Task 6: Translations — add new msgids and compile

The view now uses three new `_()`-wrapped strings, and the template introduces many more (eye-toggle labels, requirement labels, strength labels, etc.). Add catalog entries for both locales, with Chinese translations, and recompile the `.mo` files.

**Files:**
- Modify: `translations/en/LC_MESSAGES/messages.po`
- Modify: `translations/zh/LC_MESSAGES/messages.po`
- Modify: `translations/en/LC_MESSAGES/messages.mo` (generated)
- Modify: `translations/zh/LC_MESSAGES/messages.mo` (generated)

- [ ] **Step 1: Append the new English entries to `translations/en/LC_MESSAGES/messages.po`**

Append the following block to the very end of the file (after the existing last entry):

```
msgid "Current password is incorrect."
msgstr "Current password is incorrect."

msgid "New password must be different from your current password."
msgstr "New password must be different from your current password."

msgid "Password must contain both letters and numbers."
msgstr "Password must contain both letters and numbers."

msgid "Change Password · Keydion"
msgstr "Change Password · Keydion"

msgid "Account"
msgstr "Account"

msgid "Change password"
msgstr "Change password"

msgid "Set a password"
msgstr "Set a password"

msgid "Update the password used to sign in to Keydion with your email or username."
msgstr "Update the password used to sign in to Keydion with your email or username."

msgid "Create a password so you can sign in with email and password in the future."
msgstr "Create a password so you can sign in with email and password in the future."

msgid "Current password"
msgstr "Current password"

msgid "Enter your current password"
msgstr "Enter your current password"

msgid "New password"
msgstr "New password"

msgid "At least 6 characters"
msgstr "At least 6 characters"

msgid "Confirm new password"
msgstr "Confirm new password"

msgid "Re-enter your new password"
msgstr "Re-enter your new password"

msgid "Show password"
msgstr "Show password"

msgid "Hide password"
msgstr "Hide password"

msgid "Passwords match"
msgstr "Passwords match"

msgid "Passwords don’t match"
msgstr "Passwords don’t match"

msgid "Password requirements"
msgstr "Password requirements"

msgid "Letters and numbers"
msgstr "Letters and numbers"

msgid "One symbol (recommended)"
msgstr "One symbol (recommended)"

msgid "Different from your current password"
msgstr "Different from your current password"

msgid "Update password"
msgstr "Update password"

msgid "Set password"
msgstr "Set password"

msgid "Cancel"
msgstr "Cancel"

msgid "Too short"
msgstr "Too short"

msgid "Weak"
msgstr "Weak"

msgid "Fair"
msgstr "Fair"

msgid "Good"
msgstr "Good"

msgid "Strong"
msgstr "Strong"
```

Note: `Cancel` may already exist elsewhere in the catalog. After appending, run:

```bash
awk '/^msgid /{c[$0]++} END {for (k in c) if (c[k]>1) print "DUPLICATE:", k}' translations/en/LC_MESSAGES/messages.po
```

If any line prints `DUPLICATE:`, remove the duplicate `msgid`/`msgstr` pair you just added (keep the pre-existing one).

- [ ] **Step 2: Append the same set to `translations/zh/LC_MESSAGES/messages.po` with Chinese translations**

Append the following block to the very end of `translations/zh/LC_MESSAGES/messages.po`:

```
msgid "Current password is incorrect."
msgstr "当前密码不正确。"

msgid "New password must be different from your current password."
msgstr "新密码必须与当前密码不同。"

msgid "Password must contain both letters and numbers."
msgstr "密码必须包含字母和数字。"

msgid "Change Password · Keydion"
msgstr "修改密码 · Keydion"

msgid "Account"
msgstr "账户"

msgid "Change password"
msgstr "修改密码"

msgid "Set a password"
msgstr "设置密码"

msgid "Update the password used to sign in to Keydion with your email or username."
msgstr "更新用于使用邮箱或用户名登录 Keydion 的密码。"

msgid "Create a password so you can sign in with email and password in the future."
msgstr "创建一个密码，以便日后可以使用邮箱和密码登录。"

msgid "Current password"
msgstr "当前密码"

msgid "Enter your current password"
msgstr "请输入当前密码"

msgid "New password"
msgstr "新密码"

msgid "At least 6 characters"
msgstr "至少 6 个字符"

msgid "Confirm new password"
msgstr "确认新密码"

msgid "Re-enter your new password"
msgstr "请再次输入新密码"

msgid "Show password"
msgstr "显示密码"

msgid "Hide password"
msgstr "隐藏密码"

msgid "Passwords match"
msgstr "密码一致"

msgid "Passwords don’t match"
msgstr "两次输入的密码不一致"

msgid "Password requirements"
msgstr "密码要求"

msgid "Letters and numbers"
msgstr "字母和数字"

msgid "One symbol (recommended)"
msgstr "一个符号（推荐）"

msgid "Different from your current password"
msgstr "与当前密码不同"

msgid "Update password"
msgstr "更新密码"

msgid "Set password"
msgstr "设置密码"

msgid "Cancel"
msgstr "取消"

msgid "Too short"
msgstr "太短"

msgid "Weak"
msgstr "较弱"

msgid "Fair"
msgstr "一般"

msgid "Good"
msgstr "良好"

msgid "Strong"
msgstr "强"
```

Run the same duplicate check:

```bash
awk '/^msgid /{c[$0]++} END {for (k in c) if (c[k]>1) print "DUPLICATE:", k}' translations/zh/LC_MESSAGES/messages.po
```

Remove any duplicates the same way.

- [ ] **Step 3: Compile the catalogs**

Run: `python tools/compile_translations.py`

Expected: the script reports updated `.mo` files for both `en` and `zh` (no errors).

- [ ] **Step 4: Verify the new strings made it into the `.mo` binaries**

Run: `python -c "import gettext; t = gettext.translation('messages', 'translations', ['zh']); print(t.gettext('Current password is incorrect.'))"`

Expected output: `当前密码不正确。`

Also: `python -c "import gettext; t = gettext.translation('messages', 'translations', ['zh']); print(t.gettext('At least 6 characters'))"`

Expected output: `至少 6 个字符`

- [ ] **Step 5: Commit**

```bash
git add translations/en/LC_MESSAGES/messages.po translations/zh/LC_MESSAGES/messages.po translations/en/LC_MESSAGES/messages.mo translations/zh/LC_MESSAGES/messages.mo
git commit -m "i18n(account): translate change-password page strings"
```

---

## Task 7: Run the full test suite and smoke-check the dev server

Make sure the route changes didn't break anything else, and verify the end-to-end UX manually.

**Files:** none modified.

- [ ] **Step 1: Run the full unittest suite**

Run: `python -m unittest discover -s tests -p "test_*.py" -v`

Expected: all tests pass, including the 7 new ones in `tests/test_change_password_contract.py`. If any pre-existing test fails, read the failure carefully — it likely points at a real regression introduced by the route restructure (e.g., the lifted `has_password` computation).

- [ ] **Step 2: Start the dev server**

In a separate terminal:

```bash
./start_local.sh
```

Wait for the "Running on http://127.0.0.1:..." line.

- [ ] **Step 3: Manually verify the flow**

Sign in as a local user, navigate to **Dashboard → Change password**, and verify each case:

| Scenario | Expected |
|----------|----------|
| Wrong current password | Red danger flash "Current password is incorrect." at top of card; URL still `/dashboard/account/change-password`; new/confirm fields cleared on re-render |
| Mismatched new + confirm | Yellow warning flash "Passwords do not match." |
| New password 5 chars | Yellow warning "Password must be at least 6 characters." |
| New password 6 chars, letters only | Yellow warning "Password must contain both letters and numbers." |
| New password = current | Yellow warning "New password must be different from your current password." |
| Correct current + valid new | Green success flash "Password updated successfully." — visible inside the same card, no full-page navigation, sidebar `Change password` item still highlighted |

- [ ] **Step 4: Sign out and verify the new password works for login**

Confirm you can log back in with the new password.

- [ ] **Step 5: (No commit — this task is verification only.)**

---

## Self-review checklist

After all tasks complete:

- **Spec coverage:** every numbered section in the spec maps to a task above. (Section 1 → all tasks; Section 3 → Tasks 2–4; Section 4 → Task 1; Section 5 → covered by Task 7 manual check; Section 6 → Task 6; Section 7 → Tasks 2–5.)
- **Out-of-scope items from spec §8** (rate-limiting, session invalidation, email notifications, symbol enforcement, legacy redirect) are intentionally untouched.
- **No placeholders:** every step shows exact code, exact commands, exact expected output.
- **Type/name consistency:** `has_password`, `current_password`, `new_password`, `confirm_password`, `verify_password`, `get_local_user`, `get_ms_user`, `ms_record`, `local_record`, `stored_hash` — used consistently across Tasks 3 and 4.
