"""Domain route registration. Each domain module exposes register_routes(app)."""


def register_all(app):
    # Domain modules are imported here (not at package import) so importing
    # `routes` stays cheap and side-effect-free.
    from routes import resources

    resources.register_routes(app)
