"""Domain route registration. Each domain module exposes register_routes(app)."""


def register_all(app):
    # Domain modules are imported here (not at package import) so importing
    # `routes` stays cheap and side-effect-free.
    from routes import resources, guides, news, journals, upload, submissions, ai, papers, version

    resources.register_routes(app)
    guides.register_routes(app)
    news.register_routes(app)
    journals.register_routes(app)
    upload.register_routes(app)
    submissions.register_routes(app)
    ai.register_routes(app)
    papers.register_routes(app)
    version.register_routes(app)
