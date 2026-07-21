import multiprocessing
import os

bind = os.environ.get("GUNICORN_BIND", "unix:/tmp/keydion.sock")
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
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s %(L)s "%(f)s"'


def post_fork(server, worker):
    import db

    engine = db.get_engine()
    if engine is not None:
        engine.dispose()
    import app as app_module
    try:
        with app_module.app.app_context():
            app_module.rag_index.warm()   # pre-warm the vector snapshot
    except Exception:
        worker.log.exception("vector pre-warm failed")
