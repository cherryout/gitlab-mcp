#!/usr/bin/env python3
"""Mock Jenkins API server for testing jenkins-plugin.

Simulates Jenkins REST API endpoints:
  - GET /job/{path}/api/json — job info with builds
  - POST /mock/reset — clear state
  - POST /mock/build/{job_path} — add a build
  - POST /mock/update_build — update build status
  - GET /mock/state — dump state
"""

import base64
import json
import re
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any
from urllib.parse import unquote


class MockJenkinsState:
    def __init__(self):
        self.lock = threading.Lock()
        self.jobs: dict[str, list[dict]] = {}  # job_path -> builds
        self.expected_username = "testuser"
        self.expected_token = "testtoken"
        self.request_log: list[dict] = []

    def reset(self):
        with self.lock:
            self.jobs.clear()
            self.request_log.clear()

    def add_build(self, job_path: str, build: dict):
        with self.lock:
            self.jobs.setdefault(job_path, []).append(build)

    def update_build(self, job_path: str, build_number: int, updates: dict):
        with self.lock:
            for b in self.jobs.get(job_path, []):
                if b["number"] == build_number:
                    b.update(updates)
                    return True
        return False


def make_build(
    number: int,
    result: str | None = None,
    building: bool = False,
    duration: int = 0,
    timestamp: int = 1700000000000,
    display_name: str | None = None,
) -> dict:
    return {
        "number": number,
        "result": result,
        "building": building,
        "timestamp": timestamp,
        "duration": duration,
        "url": f"https://jenkins.test/job/test/{number}/",
        "displayName": display_name or f"#{number}",
    }


class MockJenkinsHandler(BaseHTTPRequestHandler):
    state: MockJenkinsState

    def log_message(self, format, *args):
        pass

    def _check_auth(self) -> bool:
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            self._respond(401, {"error": "unauthorized"})
            return False
        decoded = base64.b64decode(auth[6:]).decode()
        username, token = decoded.split(":", 1)
        if username != self.state.expected_username or token != self.state.expected_token:
            self._respond(401, {"error": "unauthorized"})
            return False
        return True

    def _respond(self, status: int, body: Any):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def do_GET(self):
        with self.state.lock:
            self.state.request_log.append({"method": "GET", "path": self.path})

        if self.path == "/mock/state":
            with self.state.lock:
                self._respond(200, {
                    "jobs": {k: len(v) for k, v in self.state.jobs.items()},
                    "request_log": self.state.request_log,
                })
            return

        if not self._check_auth():
            return

        # Match /job/a/job/b/.../api/json
        match = re.match(r"(/(?:job/[^/]+/)+)api/json", self.path.split("?")[0])
        if match:
            raw_path = match.group(1)
            # Extract job path: /job/a/job/b/ -> a/b
            parts = [unquote(p) for p in raw_path.strip("/").split("/") if p != "job"]
            job_path = "/".join(parts)

            with self.state.lock:
                builds = list(self.state.jobs.get(job_path, []))

            builds.sort(key=lambda b: b["number"], reverse=True)

            self._respond(200, {
                "name": parts[-1] if parts else job_path,
                "url": f"https://jenkins.test/{raw_path}",
                "builds": builds[:5],
            })
            return

        self._respond(404, {"error": "not found"})

    def do_POST(self):
        with self.state.lock:
            self.state.request_log.append({"method": "POST", "path": self.path})

        body = self._read_body()

        if self.path == "/mock/reset":
            self.state.reset()
            self._respond(200, {"status": "reset"})
            return

        if self.path.startswith("/mock/build/"):
            job_path = self.path[len("/mock/build/"):]
            self.state.add_build(job_path, body)
            self._respond(201, {"status": "added"})
            return

        if self.path == "/mock/update_build":
            job_path = body.get("job_path", "")
            build_number = body.get("build_number", 0)
            updates = body.get("updates", {})
            found = self.state.update_build(job_path, build_number, updates)
            self._respond(200 if found else 404, {"found": found})
            return

        self._respond(404, {"error": "not found"})


def create_mock_server(port: int = 0) -> tuple[HTTPServer, MockJenkinsState]:
    state = MockJenkinsState()

    class Handler(MockJenkinsHandler):
        pass

    Handler.state = state
    server = HTTPServer(("127.0.0.1", port), Handler)
    return server, state


def start_mock_server(port: int = 0) -> tuple[HTTPServer, MockJenkinsState, int]:
    server, state = create_mock_server(port)
    actual_port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, state, actual_port


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9998
    server, state, actual_port = start_mock_server(port)
    state.add_build("banking/sdk-backend", make_build(1, building=True))
    print(f"Mock Jenkins on http://127.0.0.1:{actual_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
