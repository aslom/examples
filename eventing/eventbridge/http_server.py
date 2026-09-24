"""stdlib WSGI server (wsgiref.simple_server + ThreadingMixIn)."""
from __future__ import annotations

from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True
    allow_reuse_address = True


class QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        return


def make(addr: tuple[str, int], app) -> ThreadingWSGIServer:
    host, port = addr
    return make_server(host, port, app,
                       server_class=ThreadingWSGIServer,
                       handler_class=QuietHandler)
