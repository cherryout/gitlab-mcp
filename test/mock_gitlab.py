#!/usr/bin/env python3
"""Mock GitLab API server for testing gitlab-channel MCP server.

Simulates the GitLab REST API endpoints that gitlab-channel.ts polls and calls:
  - GET  /api/v4/todos                              — pending todos
  - GET  /api/v4/projects                           — project discovery
  - GET  /api/v4/projects/:id/pipelines             — failed pipelines
  - POST /api/v4/projects/:id/merge_requests/:iid/notes — MR comment
  - POST /api/v4/projects/:id/issues/:iid/notes     — issue comment
  - POST /api/v4/todos/:id/mark_as_done             — dismiss todo

State is mutable at runtime via control endpoints (POST /mock/*) so tests
can inject todos, pipelines, and verify posted comments without restart.
"""

import json
import re
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any


class MockGitLabState:
    """Shared mutable state for the mock server."""

    def __init__(self):
        self.lock = threading.Lock()
        self.todos: list[dict] = []
        self.projects: list[dict] = []
        self.pipelines: dict[str, list[dict]] = {}  # project_id -> pipelines
        self.posted_notes: list[dict] = []
        self.marked_done_todos: list[str] = []
        self.expected_token: str = "test-token"
        self.request_log: list[dict] = []

    def reset(self):
        with self.lock:
            self.todos.clear()
            self.projects.clear()
            self.pipelines.clear()
            self.posted_notes.clear()
            self.marked_done_todos.clear()
            self.request_log.clear()

    def add_todo(self, todo: dict):
        with self.lock:
            self.todos.append(todo)

    def add_project(self, project: dict):
        with self.lock:
            self.projects.append(project)

    def add_pipeline(self, project_id: str, pipeline: dict):
        with self.lock:
            self.pipelines.setdefault(project_id, []).append(pipeline)

    def update_pipeline_status(self, project_id: str, pipeline_id: int, status: str):
        with self.lock:
            for p in self.pipelines.get(project_id, []):
                if p["id"] == pipeline_id:
                    p["status"] = status
                    return True
        return False


def make_todo(
    todo_id: int,
    action: str = "mentioned",
    target_type: str = "MergeRequest",
    target_iid: int = 1,
    target_title: str = "Fix bug",
    project_id: int = 100,
    project_path: str = "group/project",
    author_username: str = "reviewer",
    author_name: str = "Code Reviewer",
    body: str = "Please review this MR",
) -> dict:
    return {
        "id": todo_id,
        "action_name": action,
        "target_type": target_type,
        "target": {
            "iid": target_iid,
            "title": target_title,
            "description": "",
        },
        "project": {
            "id": project_id,
            "path_with_namespace": project_path,
        },
        "author": {
            "username": author_username,
            "name": author_name,
        },
        "body": body,
        "state": "pending",
        "created_at": "2026-04-07T10:00:00Z",
        "target_url": f"https://gitlab.example.com/{project_path}/-/merge_requests/{target_iid}",
    }


def make_project(project_id: int, path: str = "group/project") -> dict:
    return {
        "id": project_id,
        "path_with_namespace": path,
        "archived": False,
    }


def make_pipeline(
    pipeline_id: int,
    ref: str = "main",
    status: str = "failed",
    sha: str = "abcdef1234567890",
) -> dict:
    return {
        "id": pipeline_id,
        "iid": pipeline_id,
        "status": status,
        "ref": ref,
        "sha": sha,
        "web_url": f"https://gitlab.example.com/pipelines/{pipeline_id}",
        "created_at": "2026-04-07T10:00:00Z",
        "updated_at": "2026-04-07T10:05:00Z",
    }


class MockGitLabHandler(BaseHTTPRequestHandler):
    state: MockGitLabState

    def log_message(self, format, *args):
        pass

    def _check_auth(self) -> bool:
        token = self.headers.get("PRIVATE-TOKEN", "")
        if token != self.state.expected_token:
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

    def _log_request(self, method: str):
        with self.state.lock:
            self.state.request_log.append({"method": method, "path": self.path})

    def do_GET(self):
        self._log_request("GET")

        # --- Control endpoints (no auth) ---
        if self.path == "/mock/state":
            with self.state.lock:
                self._respond(200, {
                    "todos": len(self.state.todos),
                    "projects": len(self.state.projects),
                    "posted_notes": self.state.posted_notes,
                    "marked_done_todos": self.state.marked_done_todos,
                    "request_log": self.state.request_log,
                })
            return

        if self.path == "/mock/notes":
            with self.state.lock:
                self._respond(200, self.state.posted_notes)
            return

        if not self._check_auth():
            return

        # --- GitLab API endpoints ---
        if self.path.startswith("/api/v4/todos"):
            with self.state.lock:
                pending = [t for t in self.state.todos if t["state"] == "pending"]
            self._respond(200, pending)
            return

        if self.path.startswith("/api/v4/projects") and "/pipelines" in self.path:
            match = re.match(r"/api/v4/projects/([^/]+)/pipelines", self.path)
            if match:
                project_id = match.group(1)
                ref_match = re.search(r"[?&]ref=([^&]+)", self.path)
                ref_filter = ref_match.group(1) if ref_match else None
                with self.state.lock:
                    pipelines = self.state.pipelines.get(project_id, [])
                    if ref_filter:
                        from urllib.parse import unquote
                        ref_filter = unquote(ref_filter)
                        pipelines = [p for p in pipelines if p.get("ref") == ref_filter]
                self._respond(200, pipelines)
                return

        if re.match(r"/api/v4/projects\b", self.path) and "/pipelines" not in self.path:
            page_match = re.search(r"[?&]page=(\d+)", self.path)
            page = int(page_match.group(1)) if page_match else 1
            per_page_match = re.search(r"[?&]per_page=(\d+)", self.path)
            per_page = int(per_page_match.group(1)) if per_page_match else 100
            with self.state.lock:
                start = (page - 1) * per_page
                end = start + per_page
                self._respond(200, self.state.projects[start:end])
            return

        self._respond(404, {"error": "not found"})

    def do_POST(self):
        self._log_request("POST")
        body = self._read_body()

        # --- Control endpoints (no auth) ---
        if self.path == "/mock/reset":
            self.state.reset()
            self._respond(200, {"status": "reset"})
            return

        if self.path == "/mock/todo":
            self.state.add_todo(body)
            self._respond(201, {"status": "added"})
            return

        if self.path == "/mock/project":
            self.state.add_project(body)
            self._respond(201, {"status": "added"})
            return

        if self.path.startswith("/mock/pipeline/"):
            project_id = self.path.split("/")[-1]
            self.state.add_pipeline(project_id, body)
            self._respond(201, {"status": "added"})
            return

        if not self._check_auth():
            return

        # --- POST /api/v4/projects/:id/.../notes ---
        notes_match = re.match(
            r"/api/v4/projects/([^/]+)/(merge_requests|issues)/(\d+)/notes", self.path
        )
        if notes_match:
            project_id = notes_match.group(1)
            entity_type = notes_match.group(2)
            entity_iid = notes_match.group(3)
            note = {
                "project_id": project_id,
                "entity_type": entity_type,
                "entity_iid": entity_iid,
                "body": body.get("body", ""),
            }
            with self.state.lock:
                self.state.posted_notes.append(note)
            self._respond(201, {"id": len(self.state.posted_notes), "body": body.get("body", "")})
            return

        # --- POST /api/v4/todos/:id/mark_as_done ---
        todo_match = re.match(r"/api/v4/todos/(\d+)/mark_as_done", self.path)
        if todo_match:
            todo_id = todo_match.group(1)
            with self.state.lock:
                self.state.marked_done_todos.append(todo_id)
                for t in self.state.todos:
                    if str(t["id"]) == todo_id:
                        t["state"] = "done"
            self._respond(200, {"id": int(todo_id), "state": "done"})
            return

        self._respond(404, {"error": "not found"})


def create_mock_server(port: int = 0) -> tuple[HTTPServer, MockGitLabState]:
    state = MockGitLabState()

    class Handler(MockGitLabHandler):
        pass

    Handler.state = state
    server = HTTPServer(("127.0.0.1", port), Handler)
    return server, state


def start_mock_server(port: int = 0) -> tuple[HTTPServer, MockGitLabState, int]:
    server, state = create_mock_server(port)
    actual_port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, state, actual_port


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
    server, state, actual_port = start_mock_server(port)
    state.add_project(make_project(100, "group/my-project"))
    state.add_todo(make_todo(1, action="mentioned", body="@you please look at this"))
    state.add_pipeline("100", make_pipeline(500, ref="main", status="failed"))
    print(f"Mock GitLab running on http://127.0.0.1:{actual_port}")
    print("Control endpoints:")
    print(f"  POST /mock/reset       — clear all state")
    print(f"  POST /mock/todo        — add a todo (JSON body)")
    print(f"  POST /mock/project     — add a project (JSON body)")
    print(f"  POST /mock/pipeline/ID — add failed pipeline to project ID")
    print(f"  GET  /mock/state       — dump current state")
    print(f"  GET  /mock/notes       — list posted comments")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
