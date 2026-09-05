"""Admin AI-models control panel routes."""
from flask import jsonify, redirect, render_template, request, url_for

from services import llm_admin
from services.auth import require_login


def register_routes(app):

    @app.route("/dashboard/admin/models")
    def admin_models():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        return render_template("admin_models.html", user=user, snap=llm_admin.snapshot())

    @app.route("/dashboard/admin/models/save", methods=["POST"], endpoint="admin_models_save")
    def admin_models_save():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        try:
            result = llm_admin.apply_slot(
                data,
                expected_env_mtime=_mtime(data, "env_mtime"),
            )
        except llm_admin.LLMAdminConflict as exc:
            return jsonify(error=str(exc)), 409
        except llm_admin.LLMAdminError as exc:
            return jsonify(error=str(exc)), 400
        return jsonify(result)


def _mtime(data: dict, field: str) -> float | None:
    value = data.get(field)
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None
