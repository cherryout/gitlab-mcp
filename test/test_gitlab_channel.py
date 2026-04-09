#!/usr/bin/env python3
"""Comprehensive E2E tests for gitlab-channel MCP server (v0.2.0 — SQLite state).

Spins up a mock GitLab API server, connects the channel via MCP stdio,
and verifies diff-based event behavior:
  1.  MCP handshake & channel capability
  2.  Tool discovery (3 tools)
  3.  Todo created events
  4.  Todo resolved events (todo disappears from pending)
  5.  Pipeline status changed events (diff-based)
  6.  Project auto-discovery
  7.  gitlab_reply tool (MR + issue)
  8.  gitlab_mark_todo_done tool
  9.  Deduplication (same state = no event)
  10. Auth header forwarding
  11. Error handling
  12. Multiple todo actions
  13. Notification content format
  14. Watch pipeline — success
  15. Watch pipeline — failure
  16. Watch pipeline — duplicate watch rejected
  17. Watch pipeline — auto-remove after completion

Usage:
  python3 test/test_gitlab_channel.py
"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from mcp_client import MCPClient
from mock_gitlab import (
    start_mock_server,
    make_todo,
    make_project,
    make_pipeline,
)

CHANNEL_SCRIPT = str(Path(__file__).parent.parent / "gitlab-channel.ts")

PASS = 0
FAIL = 0


def ok(name: str):
    global PASS
    PASS += 1
    print(f"  PASS  {name}")


def fail(name: str, detail: str = ""):
    global FAIL
    FAIL += 1
    msg = f"  FAIL  {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)


def assert_true(condition: bool, name: str, detail: str = ""):
    if condition:
        ok(name)
    else:
        fail(name, detail)


async def create_channel_client(
    mock_port: int, token: str = "test-token", poll_interval: str = "600000",
) -> MCPClient:
    db_file = tempfile.mktemp(suffix=".db")
    env = {
        "GITLAB_API_URL": f"http://127.0.0.1:{mock_port}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": token,
        "GITLAB_CHANNEL_POLL_INTERVAL": poll_interval,
        "GITLAB_CHANNEL_DB_PATH": db_file,
    }
    client = MCPClient("npx", ["tsx", CHANNEL_SCRIPT], env)
    await client.connect()
    return client


async def collect_notifications(client: MCPClient, duration: float = 3.0) -> list[dict]:
    collected = []
    client._on_notification = lambda msg: collected.append(msg)
    await asyncio.sleep(duration)
    client._on_notification = None
    return collected


def find_events(notifications: list[dict], event_type: str) -> list[dict]:
    return [
        n for n in notifications
        if n.get("method") == "notifications/claude/channel"
        and n.get("params", {}).get("meta", {}).get("event_type") == event_type
    ]


# ─── 1. MCP Handshake ─────────────────────────────────────────────────

async def test_handshake(mock_port: int):
    print("\n--- 1. MCP Handshake & Channel Capability ---")
    client = await create_channel_client(mock_port)
    resp = await client.initialize(channel=True)
    result = resp.get("result", {})
    caps = result.get("capabilities", {})
    experimental = caps.get("experimental", {})

    assert_true("claude/channel" in experimental, "server declares claude/channel capability")
    assert_true("tools" in caps, "server declares tools capability")
    assert_true(result.get("serverInfo", {}).get("name") == "gitlab-channel", "server name correct")

    await client.close()


# ─── 2. Tool Discovery ────────────────────────────────────────────────

async def test_tool_discovery(mock_port: int):
    print("\n--- 2. Tool Discovery ---")
    client = await create_channel_client(mock_port)
    await client.initialize(channel=True)
    tools = await client.list_tools()
    names = {t["name"] for t in tools}

    assert_true("gitlab_reply" in names, "gitlab_reply exists")
    assert_true("gitlab_mark_todo_done" in names, "gitlab_mark_todo_done exists")
    assert_true("gitlab_watch_pipeline" in names, "gitlab_watch_pipeline exists")
    assert_true(len(tools) == 3, f"exactly 3 tools (got {len(tools)})")

    await client.close()


# ─── 3. Todo Created Events ───────────────────────────────────────────

async def test_todo_created(mock_port: int, state):
    print("\n--- 3. Todo Created Events ---")
    state.reset()
    state.add_todo(make_todo(todo_id=101, action="mentioned", target_title="Existing bug",
                             author_username="alice", author_name="Alice Smith", body="old"))

    client = await create_channel_client(mock_port, poll_interval="1000")
    await client.initialize(channel=True)

    # First poll seeds DB — no events emitted
    first = await collect_notifications(client, duration=3.0)
    assert_true(len(find_events(first, "todo_created")) == 0, "first poll: no events (seeding)")

    # New todo appears after seeding
    state.add_todo(make_todo(todo_id=102, action="assigned", target_title="Deploy service",
                             author_username="bob", author_name="Bob", body="Assigned"))

    second = await collect_notifications(client, duration=4.0)
    events = find_events(second, "todo_created")

    assert_true(len(events) >= 1, f"new todo_created after seed (got {len(events)})")

    if events:
        meta = events[0]["params"]["meta"]
        content = events[0]["params"]["content"]
        assert_true(meta.get("todo_id") == "102", "new todo id 102")
        assert_true(meta.get("action") == "assigned", "action is assigned")
        assert_true("Bob" in content, "content has author name")
        assert_true("Deploy service" in content, "content has title")

    await client.close()


# ─── 4. Todo Resolved Events ──────────────────────────────────────────

async def test_todo_resolved(mock_port: int, state):
    print("\n--- 4. Todo Resolved Events ---")
    state.reset()
    state.add_todo(make_todo(todo_id=201, body="Will resolve"))

    client = await create_channel_client(mock_port, poll_interval="1000")
    await client.initialize(channel=True)

    # First poll seeds DB silently
    await collect_notifications(client, duration=3.0)

    # Remove the todo (simulates user resolving it on GitLab)
    with state.lock:
        state.todos = [t for t in state.todos if t["id"] != 201]

    resolved = await collect_notifications(client, duration=4.0)
    resolved_events = find_events(resolved, "todo_resolved")
    assert_true(len(resolved_events) >= 1, f"todo_resolved received (got {len(resolved_events)})")

    if resolved_events:
        assert_true(
            resolved_events[0]["params"]["meta"].get("todo_id") == "201",
            "resolved todo id is 201",
        )

    await client.close()


# ─── 5. Pipeline Status Changed Events ────────────────────────────────

async def test_pipeline_status_changed(mock_port: int, state):
    print("\n--- 5. Pipeline Status Changed Events ---")
    state.reset()
    state.add_project(make_project(300, "backend/api"))
    state.add_pipeline("300", make_pipeline(pipeline_id=5001, ref="main", status="running"))

    client = await create_channel_client(mock_port, poll_interval="1000")
    await client.initialize(channel=True)

    # First poll — pipeline is running, stored in DB, no change event (first seen)
    await collect_notifications(client, duration=4.0)

    # Change status to failed
    state.update_pipeline_status("300", 5001, "failed")
    notifications = await collect_notifications(client, duration=4.0)
    changed = find_events(notifications, "pipeline_status_changed")

    assert_true(len(changed) >= 1, f"pipeline_status_changed received (got {len(changed)})")

    if changed:
        meta = changed[0]["params"]["meta"]
        content = changed[0]["params"]["content"]
        assert_true(meta.get("old_status") == "running", "old_status is running")
        assert_true(meta.get("new_status") == "failed", "new_status is failed")
        assert_true(meta.get("pipeline_id") == "5001", "pipeline_id correct")
        assert_true("→" in content, "content shows transition arrow")

    await client.close()


# ─── 6. Project Auto-Discovery ────────────────────────────────────────

async def test_project_discovery(mock_port: int, state):
    print("\n--- 6. Project Auto-Discovery ---")
    state.reset()
    for i in range(5):
        state.add_project(make_project(400 + i, f"team/svc-{i}"))
    state.add_pipeline("402", make_pipeline(pipeline_id=6001, ref="main", status="running"))

    client = await create_channel_client(mock_port, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=5.0)

    # Change pipeline to failed — should trigger status_changed
    state.update_pipeline_status("402", 6001, "failed")
    notifications = await collect_notifications(client, duration=5.0)
    changed = find_events(notifications, "pipeline_status_changed")

    assert_true(len(changed) >= 1, f"discovered project and tracked pipeline (got {len(changed)})")

    with state.lock:
        discovery_reqs = [r for r in state.request_log if "membership=true" in r["path"]]
    assert_true(len(discovery_reqs) >= 1, "projects discovery endpoint called")

    await client.close()


# ─── 7. gitlab_reply Tool ─────────────────────────────────────────────

async def test_reply_tool(mock_port: int, state):
    print("\n--- 7. gitlab_reply Tool ---")
    state.reset()
    client = await create_channel_client(mock_port)
    await client.initialize(channel=True)

    result = await client.call_tool("gitlab_reply", {"project_id": "100", "mr_iid": "42", "text": "LGTM"})
    assert_true(isinstance(result, str) and "Comment posted" in result, "MR reply works")

    with state.lock:
        notes = list(state.posted_notes)
    assert_true(len(notes) == 1, "1 note posted")
    if notes:
        assert_true(notes[0]["entity_type"] == "merge_requests", "posted to merge_requests")
        assert_true(notes[0]["body"] == "LGTM", "body matches")

    result = await client.call_tool("gitlab_reply", {"project_id": "200", "issue_iid": "7", "text": "On it"})
    assert_true(isinstance(result, str) and "Comment posted" in result, "issue reply works")

    with state.lock:
        assert_true(len(state.posted_notes) == 2, "2 notes total")
        assert_true(state.posted_notes[1]["entity_type"] == "issues", "second posted to issues")

    await client.close()


# ─── 8. gitlab_mark_todo_done Tool ────────────────────────────────────

async def test_mark_todo_done(mock_port: int, state):
    print("\n--- 8. gitlab_mark_todo_done Tool ---")
    state.reset()
    client = await create_channel_client(mock_port)
    await client.initialize(channel=True)

    result = await client.call_tool("gitlab_mark_todo_done", {"todo_id": "501"})
    assert_true(isinstance(result, str) and "Todo marked as done" in result, "returns confirmation")
    with state.lock:
        assert_true("501" in state.marked_done_todos, "todo 501 marked in mock")

    await client.close()


# ─── 9. Deduplication ─────────────────────────────────────────────────

async def test_deduplication(mock_port: int, state):
    print("\n--- 9. Deduplication (same state = no event) ---")
    state.reset()
    state.add_todo(make_todo(todo_id=601, body="First"))
    state.add_project(make_project(700, "team/dedup"))
    state.add_pipeline("700", make_pipeline(pipeline_id=7001, ref="main", status="running"))

    client = await create_channel_client(mock_port, poll_interval="1000")
    await client.initialize(channel=True)

    # First poll seeds DB
    await collect_notifications(client, duration=3.0)

    # Second poll — same state, no events
    second = await collect_notifications(client, duration=3.0)
    assert_true(len(find_events(second, "todo_created")) == 0, "no duplicate todo_created")
    assert_true(len(find_events(second, "pipeline_status_changed")) == 0, "no duplicate pipeline event")

    # New todo — should come through (after seeding)
    state.add_todo(make_todo(todo_id=602, body="New"))
    third = await collect_notifications(client, duration=3.0)
    new_events = find_events(third, "todo_created")
    assert_true(len(new_events) >= 1, "new todo comes through")
    if new_events:
        assert_true(new_events[0]["params"]["meta"].get("todo_id") == "602", "new todo id 602")

    await client.close()


# ─── 10. Auth Header ──────────────────────────────────────────────────

async def test_auth_header(mock_port: int, state):
    print("\n--- 10. Auth Header Forwarding ---")
    state.reset()

    # Seed with one todo, then add another after seeding
    state.add_todo(make_todo(todo_id=700))
    client = await create_channel_client(mock_port, token="test-token", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)  # seed

    state.add_todo(make_todo(todo_id=701))
    notifications = await collect_notifications(client, duration=3.0)
    assert_true(len(find_events(notifications, "todo_created")) >= 1, "correct token: events received")
    await client.close()

    # Wrong token — nothing at all
    state.reset()
    state.add_todo(make_todo(todo_id=702))
    client = await create_channel_client(mock_port, token="wrong-token", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)  # seed attempt (fails silently)
    state.add_todo(make_todo(todo_id=703))
    notifications = await collect_notifications(client, duration=3.0)
    assert_true(len(find_events(notifications, "todo_created")) == 0, "wrong token: no events")
    await client.close()


# ─── 11. Error Handling ───────────────────────────────────────────────

async def test_error_handling(mock_port: int, state):
    print("\n--- 11. Error Handling ---")
    state.reset()
    client = await create_channel_client(mock_port)
    await client.initialize(channel=True)

    resp = await client.request("tools/call", {"name": "nonexistent", "arguments": {}})
    assert_true("error" in resp, "unknown tool returns error")

    await client.close()


# ─── 12. Multiple Todo Actions ────────────────────────────────────────

async def test_todo_actions(mock_port: int, state):
    print("\n--- 12. Multiple Todo Actions ---")
    state.reset()
    # Seed with a dummy todo first
    state.add_todo(make_todo(todo_id=799, action="mentioned", body="seed"))

    client = await create_channel_client(mock_port, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)  # seed

    actions = ["mentioned", "assigned", "approval_required", "review_requested", "marked"]
    for i, action in enumerate(actions):
        state.add_todo(make_todo(todo_id=800 + i, action=action, target_title=f"Task {action}",
                                 author_username=f"u_{action}", author_name=f"User {action}"))

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "todo_created")

    assert_true(len(events) >= len(actions), f"all {len(actions)} actions received (got {len(events)})")
    received = {e["params"]["meta"]["action"] for e in events}
    for a in actions:
        assert_true(a in received, f"action '{a}' received")

    await client.close()


# ─── 13. Notification Content Format ──────────────────────────────────

async def test_notification_format(mock_port: int, state):
    print("\n--- 13. Notification Content Format ---")
    state.reset()
    state.add_todo(make_todo(todo_id=899, body="seed"))

    client = await create_channel_client(mock_port, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)  # seed

    state.add_todo(make_todo(todo_id=900, action="mentioned", target_title="Check format",
                             author_username="dev", author_name="Developer",
                             body="Please review", project_path="org/repo", target_iid=99))

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "todo_created")
    assert_true(len(events) >= 1, "notification received")

    if events:
        e = events[0]
        assert_true(e["method"] == "notifications/claude/channel", "correct method")
        content = e["params"]["content"]
        assert_true("Developer (@dev)" in content, "author format correct")
        assert_true("Check format" in content, "has title")

        meta = e["params"]["meta"]
        for key in ["event_type", "todo_id", "action", "target_type", "target_iid",
                     "project", "project_id", "author", "target_url"]:
            assert_true(key in meta, f"meta has '{key}'")

    await client.close()


# ─── 14. Watch Pipeline — Success ─────────────────────────────────────

async def test_watch_pipeline_success(mock_port: int, state):
    print("\n--- 14. Watch Pipeline — Success ---")
    state.reset()
    state.add_pipeline("500", make_pipeline(pipeline_id=14001, ref="int", status="running"))

    client = await create_channel_client(mock_port, poll_interval="600000")
    await client.initialize(channel=True)

    result = await client.call_tool("gitlab_watch_pipeline", {"project_id": "500", "ref": "int"})
    assert_true("Watching" in str(result), "watch accepted")

    notifications = await collect_notifications(client, duration=3.0)
    assert_true(len(find_events(notifications, "pipeline_watch_completed")) == 0, "no event while running")

    state.update_pipeline_status("500", 14001, "success")
    notifications = await collect_notifications(client, duration=15.0)
    completed = find_events(notifications, "pipeline_watch_completed")
    assert_true(len(completed) >= 1, f"completion notification (got {len(completed)})")

    if completed:
        meta = completed[0]["params"]["meta"]
        content = completed[0]["params"]["content"]
        assert_true(meta.get("status") == "success", "status is success")
        assert_true(meta.get("ref") == "int", "ref is int")
        assert_true("✅" in content, "has success emoji")

    await client.close()


# ─── 15. Watch Pipeline — Failure ─────────────────────────────────────

async def test_watch_pipeline_failure(mock_port: int, state):
    print("\n--- 15. Watch Pipeline — Failure ---")
    state.reset()
    state.add_pipeline("600", make_pipeline(pipeline_id=15001, ref="int", status="running"))

    client = await create_channel_client(mock_port, poll_interval="600000")
    await client.initialize(channel=True)
    await client.call_tool("gitlab_watch_pipeline", {"project_id": "600", "ref": "int"})

    state.update_pipeline_status("600", 15001, "failed")
    notifications = await collect_notifications(client, duration=15.0)
    completed = find_events(notifications, "pipeline_watch_completed")
    assert_true(len(completed) >= 1, f"failure notification (got {len(completed)})")

    if completed:
        assert_true(completed[0]["params"]["meta"].get("status") == "failed", "status is failed")
        assert_true("❌" in completed[0]["params"]["content"], "has failure emoji")

    await client.close()


# ─── 16. Watch Pipeline — Duplicate ───────────────────────────────────

async def test_watch_duplicate(mock_port: int, state):
    print("\n--- 16. Watch Pipeline — Duplicate ---")
    state.reset()
    client = await create_channel_client(mock_port, poll_interval="600000")
    await client.initialize(channel=True)

    r1 = await client.call_tool("gitlab_watch_pipeline", {"project_id": "700", "ref": "int"})
    assert_true("Watching" in str(r1), "first watch accepted")

    r2 = await client.call_tool("gitlab_watch_pipeline", {"project_id": "700", "ref": "int"})
    assert_true("Already watching" in str(r2), "duplicate rejected")

    await client.close()


# ─── 17. Watch Pipeline — Auto-Remove ─────────────────────────────────

async def test_watch_auto_remove(mock_port: int, state):
    print("\n--- 17. Watch Pipeline — Auto-Remove After Completion ---")
    state.reset()
    state.add_pipeline("800", make_pipeline(pipeline_id=17001, ref="int", status="success"))

    client = await create_channel_client(mock_port, poll_interval="600000")
    await client.initialize(channel=True)

    await client.call_tool("gitlab_watch_pipeline", {"project_id": "800", "ref": "int"})
    notifications = await collect_notifications(client, duration=15.0)
    assert_true(len(find_events(notifications, "pipeline_watch_completed")) >= 1, "completed immediately")

    r = await client.call_tool("gitlab_watch_pipeline", {"project_id": "800", "ref": "int"})
    assert_true("Watching" in str(r), "can re-watch after completion")

    await client.close()


# ─── Main ──────────────────────────────────────────────────────────────

async def main():
    print("GitLab Channel E2E Tests (v0.2.0 — SQLite state)")
    print("=" * 55)

    server, state, port = start_mock_server()
    print(f"Mock GitLab API on port {port}\n")

    await test_handshake(port)
    await test_tool_discovery(port)
    await test_todo_created(port, state)
    await test_todo_resolved(port, state)
    await test_pipeline_status_changed(port, state)
    await test_project_discovery(port, state)
    await test_reply_tool(port, state)
    await test_mark_todo_done(port, state)
    await test_deduplication(port, state)
    await test_auth_header(port, state)
    await test_error_handling(port, state)
    await test_todo_actions(port, state)
    await test_notification_format(port, state)
    await test_watch_pipeline_success(port, state)
    await test_watch_pipeline_failure(port, state)
    await test_watch_duplicate(port, state)
    await test_watch_auto_remove(port, state)

    server.shutdown()

    print("\n" + "=" * 55)
    total = PASS + FAIL
    print(f"Results: {PASS}/{total} passed, {FAIL} failed")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
