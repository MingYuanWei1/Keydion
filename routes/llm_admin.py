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

    @app.route("/dashboard/admin/models/probe", methods=["POST"], endpoint="admin_models_probe")
    def admin_models_probe():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        result = llm_admin.probe(request.get_json(silent=True) or {})
        return jsonify(result), (200 if result.get("ok") else 400)

    @app.route("/dashboard/admin/models/save", methods=["POST"], endpoint="admin_models_save")
    def admin_models_save():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        data = request.get_json(silent=True) or {}
        try:
            mtime = data.get("env_mtime")
            result = llm_admin.apply_slot(
                data, expected_mtime=float(mtime) if mtime not in (None, "") else None
            )
        except llm_admin.LLMAdminConflict as exc:
            return jsonify(error=str(exc)), 409
        except (llm_admin.LLMAdminError, ValueError) as exc:
            return jsonify(error=str(exc)), 400
        return jsonify(result)
