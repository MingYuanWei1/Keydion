"""Admin Version & updates routes."""
from flask import jsonify, redirect, render_template, url_for

from services.auth import require_login
from services import version as version_service


def register_routes(app):

    @app.route("/dashboard/admin/version")
    def admin_version():
        user = require_login(level=3)
        if not user:
            return redirect(url_for("login"))
        return render_template("admin_version.html", user=user, info=version_service.snapshot())

    @app.route("/dashboard/admin/version/update", methods=["POST"], endpoint="admin_version_update")
    def admin_version_update():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        ok, message = version_service.start_update()
        if not ok:
            return jsonify(error=message), 409
        return jsonify(started=True)

    @app.route("/dashboard/admin/version/status")
    def admin_version_status():
        user = require_login(level=3)
        if not user:
            return jsonify(error="Unauthorized"), 401
        return jsonify(version_service.update_status())
