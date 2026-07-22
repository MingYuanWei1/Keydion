"""Flask session-cookie expiry for fixed remembered authentication."""

from datetime import datetime, timezone

from flask.sessions import SecureCookieSessionInterface


AUTH_EXPIRES_AT_KEY = "auth_expires_at"


class AuthExpirySessionInterface(SecureCookieSessionInterface):
    def get_expiration_time(self, app, flask_session):
        raw_deadline = flask_session.get(AUTH_EXPIRES_AT_KEY)
        if flask_session.permanent and raw_deadline is not None:
            try:
                return datetime.fromtimestamp(int(raw_deadline), tz=timezone.utc)
            except (TypeError, ValueError, OverflowError):
                pass
        return super().get_expiration_time(app, flask_session)
