#!/usr/bin/env python3
"""Comprehensive E2E tests for jenkins-channel plugin.

Spins up a mock Jenkins API server, connects the channel via MCP stdio,
and verifies diff-based events:
  1.  MCP handshake & channel capability
  2.  First-run seeding (no events)
  3.  Build started event
  4.  Build completed — SUCCESS
  5.  Build completed — FAILURE
  6.  Build completed — UNSTABLE
  7.  Deduplication (same builds not sent twice)
  8.  Multiple jobs watched
  9.  Auth header forwarding
  10. Event meta fields
  11. Build duration in content
  12. Build status transition (building → completed)

Usage:
  python3 test/test_jenkins_channel.py
"""

import asyncio
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from mcp_client import MCPClient
from mock_jenkins import start_mock_server, make_build

CHANNEL_SCRIPT = str(Path(__file__).parent.parent / "jenkins-channel.ts")

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


async def create_client(
    mock_port: int,
    jobs: str,
    username: str = "testuser",
    token: str = "testtoken",
    poll_interval: str = "600000",
) -> MCPClient:
    db_file = tempfile.mktemp(suffix=".db")
    log_file = tempfile.mktemp(suffix=".log")
    env = {
        "JENKINS_CHANNEL_URL": f"http://127.0.0.1:{mock_port}",
        "JENKINS_CHANNEL_USERNAME": username,
        "JENKINS_CHANNEL_TOKEN": token,
        "JENKINS_CHANNEL_JOBS": jobs,
        "JENKINS_CHANNEL_POLL_INTERVAL": poll_interval,
        "JENKINS_CHANNEL_DB_PATH": db_file,
        "JENKINS_CHANNEL_LOG_PATH": log_file,
        "JENKINS_CHANNEL_LOG_LEVEL": "debug",
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


def all_events(notifications: list[dict]) -> list[dict]:
    return [
        n for n in notifications
        if n.get("method") == "notifications/claude/channel"
    ]


# ─── 1. MCP Handshake ─────────────────────────────────────────────────

async def test_handshake(mock_port: int):
    print("\n--- 1. MCP Handshake & Channel Capability ---")
    client = await create_client(mock_port, "test/job")
    resp = await client.initialize(channel=True)
    result = resp.get("result", {})
    caps = result.get("capabilities", {})

    assert_true("claude/channel" in caps.get("experimental", {}), "channel capability declared")
    assert_true(result.get("serverInfo", {}).get("name") == "jenkins-channel", "server name correct")

    tools = await client.list_tools()
    assert_true(len(tools) == 0, f"no tools (events only, got {len(tools)})")

    await client.close()


# ─── 2. First-Run Seeding ─────────────────────────────────────────────

async def test_seeding(mock_port: int, state):
    print("\n--- 2. First-Run Seeding (no events) ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS", duration=60000))
    state.add_build("banking/sdk", make_build(2, building=True))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)

    notifications = await collect_notifications(client, duration=4.0)
    events = all_events(notifications)
    assert_true(len(events) == 0, f"no events on first run (got {len(events)})")

    await client.close()


# ─── 3. Build Started ─────────────────────────────────────────────────

async def test_build_started(mock_port: int, state):
    print("\n--- 3. Build Started Event ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.add_build("banking/sdk", make_build(2, building=True))

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_started")

    assert_true(len(events) >= 1, f"build_started event (got {len(events)})")
    if events:
        meta = events[0]["params"]["meta"]
        content = events[0]["params"]["content"]
        assert_true(meta.get("job") == "banking/sdk", "job correct")
        assert_true(meta.get("build_number") == "2", "build_number correct")
        assert_true("started" in content.lower(), "content says started")

    await client.close()


# ─── 4. Build Completed — SUCCESS ─────────────────────────────────────

async def test_build_success(mock_port: int, state):
    print("\n--- 4. Build Completed — SUCCESS ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))
    state.add_build("banking/sdk", make_build(2, building=True))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.update_build("banking/sdk", 2, {"building": False, "result": "SUCCESS", "duration": 120000})

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_completed")

    assert_true(len(events) >= 1, f"build_completed event (got {len(events)})")
    if events:
        meta = events[0]["params"]["meta"]
        content = events[0]["params"]["content"]
        assert_true(meta.get("result") == "SUCCESS", "result is SUCCESS")
        assert_true("✅" in content, "has success emoji")
        assert_true(meta.get("build_number") == "2", "build_number correct")

    await client.close()


# ─── 5. Build Completed — FAILURE ─────────────────────────────────────

async def test_build_failure(mock_port: int, state):
    print("\n--- 5. Build Completed — FAILURE ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, building=True))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.update_build("banking/sdk", 1, {"building": False, "result": "FAILURE", "duration": 45000})

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_completed")

    assert_true(len(events) >= 1, f"build_completed FAILURE (got {len(events)})")
    if events:
        meta = events[0]["params"]["meta"]
        content = events[0]["params"]["content"]
        assert_true(meta.get("result") == "FAILURE", "result is FAILURE")
        assert_true("❌" in content, "has failure emoji")

    await client.close()


# ─── 6. Build Completed — UNSTABLE ────────────────────────────────────

async def test_build_unstable(mock_port: int, state):
    print("\n--- 6. Build Completed — UNSTABLE ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, building=True))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.update_build("banking/sdk", 1, {"building": False, "result": "UNSTABLE", "duration": 30000})

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_completed")

    assert_true(len(events) >= 1, f"build_completed UNSTABLE (got {len(events)})")
    if events:
        meta = events[0]["params"]["meta"]
        assert_true(meta.get("result") == "UNSTABLE", "result is UNSTABLE")
        assert_true("⚠️" in events[0]["params"]["content"], "has warning emoji")

    await client.close()


# ─── 7. Deduplication ─────────────────────────────────────────────────

async def test_deduplication(mock_port: int, state):
    print("\n--- 7. Deduplication ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    # Same state — no events
    second = await collect_notifications(client, duration=3.0)
    assert_true(len(all_events(second)) == 0, f"no duplicates (got {len(all_events(second))})")

    # New build — should come through
    state.add_build("banking/sdk", make_build(2, building=True))
    third = await collect_notifications(client, duration=4.0)
    assert_true(len(find_events(third, "build_started")) >= 1, "new build detected")

    # Same build still building — no duplicate
    fourth = await collect_notifications(client, duration=3.0)
    assert_true(len(all_events(fourth)) == 0, f"no duplicate for same building state (got {len(all_events(fourth))})")

    await client.close()


# ─── 8. Multiple Jobs ─────────────────────────────────────────────────

async def test_multiple_jobs(mock_port: int, state):
    print("\n--- 8. Multiple Jobs Watched ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))
    state.add_build("banking/frontend", make_build(1, result="SUCCESS"))

    client = await create_client(mock_port, "banking/sdk,banking/frontend", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.add_build("banking/sdk", make_build(2, building=True))
    state.add_build("banking/frontend", make_build(2, building=True))

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_started")
    jobs_seen = {e["params"]["meta"]["job"] for e in events}

    assert_true(len(events) >= 2, f"events from both jobs (got {len(events)})")
    assert_true("banking/sdk" in jobs_seen, "sdk build detected")
    assert_true("banking/frontend" in jobs_seen, "frontend build detected")

    await client.close()


# ─── 9. Auth Header ───────────────────────────────────────────────────

async def test_auth(mock_port: int, state):
    print("\n--- 9. Auth Header Forwarding ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))

    # Correct auth — seed works
    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.add_build("banking/sdk", make_build(2, building=True))
    notifications = await collect_notifications(client, duration=3.0)
    assert_true(len(find_events(notifications, "build_started")) >= 1, "correct auth: events received")
    await client.close()

    # Wrong auth — no events
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))

    client = await create_client(mock_port, "banking/sdk", username="wrong", token="wrong", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.add_build("banking/sdk", make_build(2, building=True))
    notifications = await collect_notifications(client, duration=3.0)
    assert_true(len(all_events(notifications)) == 0, "wrong auth: no events")
    await client.close()


# ─── 10. Event Meta Fields ────────────────────────────────────────────

async def test_event_meta(mock_port: int, state):
    print("\n--- 10. Event Meta Fields ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    state.add_build("banking/sdk", make_build(2, building=True))
    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_started")
    assert_true(len(events) >= 1, "event received")

    if events:
        meta = events[0]["params"]["meta"]
        for key in ["event_type", "job", "build_number", "url"]:
            assert_true(key in meta, f"meta has '{key}'")

    # Now complete it — check build_completed meta
    state.update_build("banking/sdk", 2, {"building": False, "result": "SUCCESS", "duration": 90000})
    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_completed")
    assert_true(len(events) >= 1, "completed event received")

    if events:
        meta = events[0]["params"]["meta"]
        for key in ["event_type", "job", "build_number", "result", "duration", "url"]:
            assert_true(key in meta, f"completed meta has '{key}'")

    await client.close()


# ─── 11. Build Duration in Content ────────────────────────────────────

async def test_duration_format(mock_port: int, state):
    print("\n--- 11. Build Duration in Content ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, building=True))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    # 2 minutes 30 seconds
    state.update_build("banking/sdk", 1, {"building": False, "result": "SUCCESS", "duration": 150000})

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "build_completed")
    assert_true(len(events) >= 1, "completed event")
    if events:
        content = events[0]["params"]["content"]
        assert_true("2m30s" in content, f"duration formatted as 2m30s (content: {content[:80]})")

    await client.close()


# ─── 12. Building → Completed Transition ──────────────────────────────

async def test_build_transition(mock_port: int, state):
    print("\n--- 12. Build Transition (started → completed) ---")
    state.reset()
    state.add_build("banking/sdk", make_build(1, result="SUCCESS"))

    client = await create_client(mock_port, "banking/sdk", poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    # Build starts
    state.add_build("banking/sdk", make_build(5, building=True))
    notifications = await collect_notifications(client, duration=4.0)
    started = find_events(notifications, "build_started")
    assert_true(len(started) >= 1, "build_started received")

    # Build completes
    state.update_build("banking/sdk", 5, {"building": False, "result": "FAILURE", "duration": 60000})
    notifications = await collect_notifications(client, duration=4.0)
    completed = find_events(notifications, "build_completed")
    assert_true(len(completed) >= 1, "build_completed received after started")
    if completed:
        assert_true(completed[0]["params"]["meta"].get("result") == "FAILURE", "result is FAILURE")
        assert_true(completed[0]["params"]["meta"].get("build_number") == "5", "same build #5")

    await client.close()


# ─── Main ──────────────────────────────────────────────────────────────

async def main():
    print("Jenkins Channel E2E Tests")
    print("=" * 55)

    server, state, port = start_mock_server()
    print(f"Mock Jenkins API on port {port}\n")

    await test_handshake(port)
    await test_seeding(port, state)
    await test_build_started(port, state)
    await test_build_success(port, state)
    await test_build_failure(port, state)
    await test_build_unstable(port, state)
    await test_deduplication(port, state)
    await test_multiple_jobs(port, state)
    await test_auth(port, state)
    await test_event_meta(port, state)
    await test_duration_format(port, state)
    await test_build_transition(port, state)

    server.shutdown()

    print("\n" + "=" * 55)
    total = PASS + FAIL
    print(f"Results: {PASS}/{total} passed, {FAIL} failed")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
