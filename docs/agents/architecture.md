# Architecture

## Application boundaries

`app.py` owns `create_app()`, global context processors and filters, retained core routes, and domain-route registration. Importing `app` must not construct the Flask application or connect to the database. `wsgi.py` is the serving entry point and constructs it with `app = create_app()`.

Put domain HTTP behavior in `routes/<domain>.py` behind `register_routes(app)` and domain logic in `services/<domain>.py`. Modules under `routes/` and `services/` must not import `app`; `tests/test_static_contracts.py` enforces this boundary.

Preserve endpoint names when moving handlers because `url_for()` calls cross domain-module boundaries. Import `config.py` before reading `os.environ`, because importing it selects and loads the environment file.

## Dashboard routes

Authenticated workspace and administration routes are canonical under `/dashboard/...`. Retained legacy GET paths redirect to their dashboard equivalents; do not add legacy mutation routes. `tests/test_dashboard_url_nesting_contract.py` owns the expected route map.

## Dashboard partial rendering

`static/js/dashboard.js` sends `X-Partial-Content: 1` and swaps the response into `#dashboardMain`; the partial contract is header-based, not query-parameter-based.

A partial-loadable dashboard template starts with:

```jinja2
{% extends "_bare.html" if partial else "_dashboard_shell.html" %}
```

The global `inject_partial_flag` context processor derives `partial` from that header. Dashboard routes should rely on the injected value rather than overriding it with route context.
