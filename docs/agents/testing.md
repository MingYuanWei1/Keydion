# Testing

Tests use `unittest` and include source/template contracts, request behavior, SQLite tests, and real-MySQL migration and concurrency coverage.

## Run tests

The CI-equivalent runner requires `PAPERQUERY_TEST_MYSQL_ADMIN_URL` for a reachable MySQL 9.x server. It creates, bootstraps, and removes a validated disposable database plus temporary data and upload directories.

Run the full suite:

```bash
python3 tools/run_isolated_tests.py discover -s tests -p "test_*.py" -v
```

Run one test file through the same isolated path:

```bash
python3 tools/run_isolated_tests.py tests/test_ee_total_grade_contract.py -v
```

Direct `unittest` discovery is not the full MySQL-backed verification path. Never point tests at a development or production database.

## Test patterns

- Source contracts use `tests/support.py` to find functions across `app.py`, `config.py`, `db.py`, `models.py`, `routes/`, and `services/`; do not assume a function still lives in `app.py`.
- Template/JavaScript contracts render Jinja or compare DOM IDs with script lookups.
- Tests calling an unmocked `create_app()` need a bootstrapped, current database.

Global CSRF protection affects test setup:

- Flask test-client tests that POST set `app.config["WTF_CSRF_ENABLED"] = False`.
- Standalone Jinja tests set `env.globals["csrf_token"] = lambda: ""`.
