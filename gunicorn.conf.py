import multiprocessing
import os

bind = os.environ.get("GUNICORN_BIND", "127.0.0.1:5000")
workers = int(os.environ.get("GUNICORN_WORKERS", (multiprocessing.cpu_count() * 2) + 1))
worker_class = os.environ.get("GUNICORN_WORKER_CLASS", "sync")
threads = int(os.environ.get("GUNICORN_THREADS", 1))
timeout = int(os.environ.get("GUNICORN_TIMEOUT", 60))
graceful_timeout = int(os.environ.get("GUNICORN_GRACEFUL_TIMEOUT", 30))
keepalive = int(os.environ.get("GUNICORN_KEEPALIVE", 5))
max_requests = int(os.environ.get("GUNICORN_MAX_REQUESTS", 1000))
max_requests_jitter = int(os.environ.get("GUNICORN_MAX_REQUESTS_JITTER", 100))
preload_app = False

accesslog = "-"
errorlog = "-"
# Deliberately omit %(r)s, %(q)s, and %(f)s: OAuth callback credentials and
# state live in the query string and referrers must not enter access logs.
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(m)s %(U)s %(H)s" %(s)s %(b)s %(L)s'


def post_fork(server, worker):
    import db

    engine = db.get_engine()
    if engine is not None:
        engine.dispose()
    import rag_index
    import wsgi
    try:
        with wsgi.app.app_context():
            rag_index.warm()   # pre-warm the vector snapshot
    except Exception:
        worker.log.exception("vector pre-warm failed")
