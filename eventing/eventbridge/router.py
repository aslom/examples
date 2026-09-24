"""Regex → handler dispatcher for the WSGI app."""
from __future__ import annotations

import re
from typing import Callable


class Route:
    def __init__(self, method: str, pattern: str, handler: Callable) -> None:
        self.method  = method.upper()
        self.pattern = re.compile("^" + pattern + "$")
        self.handler = handler


class Dispatcher:
    def __init__(self) -> None:
        self._routes: list[Route] = []

    def add(self, method: str, pattern: str, handler: Callable) -> None:
        self._routes.append(Route(method, pattern, handler))

    def dispatch(self, method: str, path: str, environ, start_response):
        for r in self._routes:
            if r.method != method.upper():
                continue
            m = r.pattern.match(path)
            if m:
                return r.handler(environ, start_response, **m.groupdict())
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"not found"]
