#!/usr/bin/env python3
"""Comprehensive E2E tests for slack-channel plugin.

Spins up a mock Slack MCP server (mock_slack_mcp.py), connects the channel
via a standalone wrapper (slack-channel.ts), and verifies diff-based events:
  1.  MCP handshake & channel capability
  2.  First-run seeding (no events)
  3.  New message → message_received event
  4.  Mention → mention event
  5.  Thread reply → thread_reply event
  6.  Alert channel → alert event
  7.  Mentions-only channel filtering
  8.  Deduplication (same messages not sent twice)
  9.  Multiple channels polled
  10. Event meta fields
  11. ACL parsing

Usage:
  python3 test/test_slack_channel.py
"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from mcp_client import MCPClient

CHANNEL_SCRIPT = str(Path(__file__).parent.parent / "slack-channel.ts")
MOCK_AUTH_SCRIPT = str(Path(__file__).parent / "mock_slack_auth.sh")

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


def write_state(path: str, channels: list, messages: dict):
    with open(path, "w") as f:
        json.dump({"channels": channels, "messages": messages}, f)


def make_acl(path: str, read: list, mentions_only: list = None):
    acl = {"channels": {"read": read}}
    if mentions_only:
        acl["channels"]["mentions_only"] = mentions_only
    with open(path, "w") as f:
        json.dump(acl, f)


async def create_client(
    state_path: str,
    acl_path: str,
    poll_interval: str = "600000",
    my_username: str = "testuser",
) -> MCPClient:
    db_file = tempfile.mktemp(suffix=".db")
    log_file = tempfile.mktemp(suffix=".log")
    env = {
        "SLACK_MCP_AUTH_BIN": MOCK_AUTH_SCRIPT,
        "SLACK_MCP_ACL_FILE": acl_path,
        "SLACK_CHANNEL_WORKSPACE": "test",
        "SLACK_CHANNEL_POLL_INTERVAL": poll_interval,
        "SLACK_CHANNEL_MY_USERNAME": my_username,
        "SLACK_CHANNEL_DB_PATH": db_file,
        "SLACK_CHANNEL_LOG_PATH": log_file,
        "SLACK_CHANNEL_LOG_LEVEL": "debug",
        "MOCK_SLACK_STATE": state_path,
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


def all_channel_events(notifications: list[dict]) -> list[dict]:
    return [
        n for n in notifications
        if n.get("method") == "notifications/claude/channel"
    ]


# ─── 1. MCP Handshake ─────────────────────────────────────────────────

async def test_handshake(state_path: str, acl_path: str):
    print("\n--- 1. MCP Handshake & Channel Capability ---")
    client = await create_client(state_path, acl_path)
    resp = await client.initialize(channel=True)
    result = resp.get("result", {})
    caps = result.get("capabilities", {})

    assert_true("claude/channel" in caps.get("experimental", {}), "channel capability declared")
    assert_true(result.get("serverInfo", {}).get("name") == "slack-channel", "server name correct")

    tools = await client.list_tools()
    assert_true(len(tools) == 0, f"no tools (events only, got {len(tools)})")

    await client.close()


# ─── 2. First-Run Seeding ─────────────────────────────────────────────

async def test_seeding(state_path: str, acl_path: str):
    print("\n--- 2. First-Run Seeding (no events) ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "alice", "ts": "1000.001", "text": "existing message"},
            {"user": "bob", "ts": "1000.002", "text": "another old one"},
        ],
    })
    make_acl(acl_path, ["#banking-dev"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)

    notifications = await collect_notifications(client, duration=4.0)
    events = all_channel_events(notifications)

    assert_true(len(events) == 0, f"no events on first run (got {len(events)})")

    await client.close()


# ─── 3. New Message → message_received ─────────────────────────────────

async def test_message_received(state_path: str, acl_path: str):
    print("\n--- 3. New Message → message_received ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "alice", "ts": "1000.001", "text": "seed message"},
        ],
    })
    make_acl(acl_path, ["#banking-dev"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)

    await collect_notifications(client, duration=3.0)

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "bob", "ts": "2000.001", "text": "new message after seed"},
            {"user": "alice", "ts": "1000.001", "text": "seed message"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "message_received")

    assert_true(len(events) >= 1, f"message_received event (got {len(events)})")

    if events:
        meta = events[0]["params"]["meta"]
        content = events[0]["params"]["content"]
        assert_true("bob" in content, "content contains user")
        assert_true("new message after seed" in content, "content contains text")
        assert_true(meta.get("channel_name") == "banking-dev", "channel_name correct")
        assert_true(meta.get("user") == "bob", "user meta correct")

    await client.close()


# ─── 4. Mention → mention Event ───────────────────────────────────────

async def test_mention(state_path: str, acl_path: str):
    print("\n--- 4. Mention → mention Event ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })
    make_acl(acl_path, ["#banking-dev"])

    client = await create_client(state_path, acl_path, poll_interval="1000", my_username="testuser")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "alice", "ts": "3000.001", "text": "hey @testuser can you review?"},
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "mention")

    assert_true(len(events) >= 1, f"mention event (got {len(events)})")
    if events:
        assert_true("testuser" in events[0]["params"]["content"].lower(), "mention content has username")

    await client.close()


# ─── 5. Thread Reply → thread_reply Event ──────────────────────────────

async def test_thread_reply(state_path: str, acl_path: str):
    print("\n--- 5. Thread Reply → thread_reply Event ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })
    make_acl(acl_path, ["#banking-dev"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "bob", "ts": "4000.001", "thread_ts": "1000.001", "text": "reply in thread"},
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "thread_reply")

    assert_true(len(events) >= 1, f"thread_reply event (got {len(events)})")
    if events:
        meta = events[0]["params"]["meta"]
        assert_true(meta.get("thread_ts") == "1000.001", "thread_ts in meta")

    await client.close()


# ─── 6. Alert Channel → message_received ──────────────────────────────

async def test_alert_channel(state_path: str, acl_path: str):
    print("\n--- 6. Alert Channel → message_received (no special type) ---")

    write_state(state_path, [
        {"id": "C099", "name": "alerts"},
    ], {
        "C099": [
            {"user": "bot", "ts": "1000.001", "text": "seed alert"},
        ],
    })
    make_acl(acl_path, ["#alerts"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    write_state(state_path, [
        {"id": "C099", "name": "alerts"},
    ], {
        "C099": [
            {"user": "grafana-bot", "ts": "5000.001", "text": "ALERT: CPU > 90% on sdk-prod"},
            {"user": "bot", "ts": "1000.001", "text": "seed alert"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "message_received")

    assert_true(len(events) >= 1, f"message_received from alert channel (got {len(events)})")
    if events:
        assert_true("CPU" in events[0]["params"]["content"], "content has alert message")
        assert_true(events[0]["params"]["meta"].get("channel_name") == "alerts", "channel_name is alerts")

    await client.close()


# ─── 7. Mentions-Only Channel Filtering ───────────────────────────────

async def test_mentions_only(state_path: str, acl_path: str):
    print("\n--- 7. Mentions-Only Channel Filtering ---")

    write_state(state_path, [
        {"id": "C010", "name": "banking-team"},
    ], {
        "C010": [
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })
    make_acl(acl_path, ["#banking-team"], mentions_only=["#banking-team"])

    client = await create_client(state_path, acl_path, poll_interval="1000", my_username="testuser")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    # Message without mention — should be filtered
    write_state(state_path, [
        {"id": "C010", "name": "banking-team"},
    ], {
        "C010": [
            {"user": "bob", "ts": "6000.001", "text": "general discussion, no mention"},
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = all_channel_events(notifications)
    assert_true(len(events) == 0, f"no event for non-mention in mentions-only channel (got {len(events)})")

    # Message with mention — should pass through
    write_state(state_path, [
        {"id": "C010", "name": "banking-team"},
    ], {
        "C010": [
            {"user": "bob", "ts": "7000.001", "text": "hey @testuser need your input"},
            {"user": "bob", "ts": "6000.001", "text": "general discussion, no mention"},
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = find_events(notifications, "mention")
    assert_true(len(events) >= 1, f"mention passes through mentions-only filter (got {len(events)})")

    await client.close()


# ─── 8. Deduplication ─────────────────────────────────────────────────

async def test_deduplication(state_path: str, acl_path: str):
    print("\n--- 8. Deduplication ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })
    make_acl(acl_path, ["#banking-dev"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    # Add new message
    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "bob", "ts": "8000.001", "text": "unique message"},
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })

    first = await collect_notifications(client, duration=4.0)
    assert_true(len(all_channel_events(first)) >= 1, "first poll: event received")

    # Same state — no new events
    second = await collect_notifications(client, duration=4.0)
    assert_true(len(all_channel_events(second)) == 0, f"second poll: no duplicates (got {len(all_channel_events(second))})")

    # Truly new message
    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "carol", "ts": "9000.001", "text": "another new one"},
            {"user": "bob", "ts": "8000.001", "text": "unique message"},
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })

    third = await collect_notifications(client, duration=4.0)
    events = all_channel_events(third)
    assert_true(len(events) >= 1, f"third poll: new message (got {len(events)})")
    if events:
        assert_true("carol" in events[0]["params"]["content"], "new message from carol")

    await client.close()


# ─── 9. Multiple Channels ─────────────────────────────────────────────

async def test_multiple_channels(state_path: str, acl_path: str):
    print("\n--- 9. Multiple Channels ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
        {"id": "C002", "name": "ops-support"},
    ], {
        "C001": [{"user": "alice", "ts": "1000.001", "text": "seed1"}],
        "C002": [{"user": "bob", "ts": "1000.002", "text": "seed2"}],
    })
    make_acl(acl_path, ["#banking-dev", "#ops-support"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
        {"id": "C002", "name": "ops-support"},
    ], {
        "C001": [
            {"user": "alice", "ts": "10000.001", "text": "msg in banking-dev"},
            {"user": "alice", "ts": "1000.001", "text": "seed1"},
        ],
        "C002": [
            {"user": "bob", "ts": "10000.002", "text": "msg in ops-support"},
            {"user": "bob", "ts": "1000.002", "text": "seed2"},
        ],
    })

    notifications = await collect_notifications(client, duration=5.0)
    events = all_channel_events(notifications)
    channels_seen = {e["params"]["meta"]["channel_name"] for e in events}

    assert_true(len(events) >= 2, f"events from multiple channels (got {len(events)})")
    assert_true("banking-dev" in channels_seen, "banking-dev event received")
    assert_true("ops-support" in channels_seen, "ops-support event received")

    await client.close()


# ─── 10. Event Meta Fields ────────────────────────────────────────────

async def test_event_meta(state_path: str, acl_path: str):
    print("\n--- 10. Event Meta Fields ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [{"user": "alice", "ts": "1000.001", "text": "seed"}],
    })
    make_acl(acl_path, ["#banking-dev"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
    ], {
        "C001": [
            {"user": "dev", "ts": "11000.001", "text": "check meta fields"},
            {"user": "alice", "ts": "1000.001", "text": "seed"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = all_channel_events(notifications)
    assert_true(len(events) >= 1, "event received for meta check")

    if events:
        meta = events[0]["params"]["meta"]
        for key in ["event_type", "channel_id", "channel_name", "user", "message_id", "ts"]:
            assert_true(key in meta, f"meta has '{key}'")
        assert_true(meta["channel_id"] == "C001", "channel_id correct")
        assert_true(meta["user"] == "dev", "user correct")

    await client.close()


# ─── 11. ACL Parsing ──────────────────────────────────────────────────

async def test_acl_filtering(state_path: str, acl_path: str):
    print("\n--- 11. ACL Channel Filtering ---")

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
        {"id": "C002", "name": "random"},
        {"id": "C003", "name": "general"},
    ], {
        "C001": [{"user": "a", "ts": "1000.001", "text": "seed"}],
        "C002": [{"user": "b", "ts": "1000.002", "text": "seed"}],
        "C003": [{"user": "c", "ts": "1000.003", "text": "seed"}],
    })
    make_acl(acl_path, ["#banking-dev"])

    client = await create_client(state_path, acl_path, poll_interval="1000")
    await client.initialize(channel=True)
    await collect_notifications(client, duration=3.0)

    write_state(state_path, [
        {"id": "C001", "name": "banking-dev"},
        {"id": "C002", "name": "random"},
        {"id": "C003", "name": "general"},
    ], {
        "C001": [
            {"user": "a", "ts": "12000.001", "text": "allowed channel msg"},
            {"user": "a", "ts": "1000.001", "text": "seed"},
        ],
        "C002": [
            {"user": "b", "ts": "12000.002", "text": "not in ACL"},
            {"user": "b", "ts": "1000.002", "text": "seed"},
        ],
        "C003": [
            {"user": "c", "ts": "12000.003", "text": "also not in ACL"},
            {"user": "c", "ts": "1000.003", "text": "seed"},
        ],
    })

    notifications = await collect_notifications(client, duration=4.0)
    events = all_channel_events(notifications)
    channels_seen = {e["params"]["meta"].get("channel_name") for e in events}

    assert_true("banking-dev" in channels_seen or len(events) >= 1, "ACL-allowed channel has events")
    assert_true("random" not in channels_seen, "non-ACL channel 'random' filtered out")
    assert_true("general" not in channels_seen, "non-ACL channel 'general' filtered out")

    await client.close()


# ─── Main ──────────────────────────────────────────────────────────────

async def main():
    print("Slack Channel E2E Tests")
    print("=" * 55)

    state_path = tempfile.mktemp(suffix=".json")
    acl_path = tempfile.mktemp(suffix=".json")

    write_state(state_path, [], {})
    make_acl(acl_path, [])

    await test_handshake(state_path, acl_path)
    await test_seeding(state_path, acl_path)
    await test_message_received(state_path, acl_path)
    await test_mention(state_path, acl_path)
    await test_thread_reply(state_path, acl_path)
    await test_alert_channel(state_path, acl_path)
    await test_mentions_only(state_path, acl_path)
    await test_deduplication(state_path, acl_path)
    await test_multiple_channels(state_path, acl_path)
    await test_event_meta(state_path, acl_path)
    await test_acl_filtering(state_path, acl_path)

    print("\n" + "=" * 55)
    total = PASS + FAIL
    print(f"Results: {PASS}/{total} passed, {FAIL} failed")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
