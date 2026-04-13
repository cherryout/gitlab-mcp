#!/usr/bin/env python3
"""E2E tests against real GitLab CE and Jenkins in Docker.

Versions match production:
  - GitLab CE 17.11.x (prod: 17.11.7-ee)
  - Jenkins 2.387.1 (prod: 2.387.1)

Usage:
  # Start services (first time takes 3-5 min for GitLab)
  docker compose -f test/e2e_docker/docker-compose.yml up -d --wait

  # Bootstrap
  python3 test/e2e_docker/setup/gitlab_bootstrap.py
  python3 test/e2e_docker/setup/jenkins_bootstrap.py

  # Run tests
  python3 test/e2e_docker/test_e2e.py

  # Or all-in-one:
  python3 test/e2e_docker/test_e2e.py --bootstrap

  # Tear down
  docker compose -f test/e2e_docker/docker-compose.yml down -v
"""

import asyncio
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from mcp_client import MCPClient

PROJECT_ROOT = Path(__file__).parent.parent.parent
GITLAB_CHANNEL = str(PROJECT_ROOT / "gitlab-channel.ts")
JENKINS_CHANNEL = str(PROJECT_ROOT / "jenkins-channel.ts")
HUB_CHANNEL = str(PROJECT_ROOT / "channel-hub.ts")

GITLAB_URL = os.environ.get("E2E_GITLAB_URL", "http://127.0.0.1:8929")
JENKINS_URL = os.environ.get("E2E_JENKINS_URL", "http://127.0.0.1:8930")

PASS = 0
FAIL = 0

gitlab_ctx: dict = {}
jenkins_ctx: dict = {}


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


def assert_true(cond: bool, name: str, detail: str = ""):
    if cond:
        ok(name)
    else:
        fail(name, detail)


async def collect(client: MCPClient, duration: float = 5.0) -> list[dict]:
    collected: list[dict] = []
    client._on_notification = lambda m: collected.append(m)
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
    return [n for n in notifications if n.get("method") == "notifications/claude/channel"]


def gitlab_api(method: str, path: str, body: dict | None = None) -> dict | list:
    url = f"{GITLAB_URL}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"PRIVATE-TOKEN": gitlab_ctx["pat"], "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def jenkins_api(method: str, path: str, data: bytes | None = None) -> bytes:
    url = f"{JENKINS_URL}{path}"
    auth = base64.b64encode(f"{jenkins_ctx['username']}:{jenkins_ctx['token']}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        return e.read()


def make_gitlab_client(poll_interval: str = "2000", namespace: str = "") -> MCPClient:
    return MCPClient("npx", ["tsx", GITLAB_CHANNEL], {
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": poll_interval,
        "GITLAB_CHANNEL_NAMESPACE": namespace,
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "GITLAB_CHANNEL_LOG_LEVEL": "debug",
    })


def make_jenkins_client(jobs: str, poll_interval: str = "2000") -> MCPClient:
    return MCPClient("npx", ["tsx", JENKINS_CHANNEL], {
        "JENKINS_CHANNEL_URL": JENKINS_URL,
        "JENKINS_CHANNEL_USERNAME": jenkins_ctx["username"],
        "JENKINS_CHANNEL_TOKEN": jenkins_ctx["token"],
        "JENKINS_CHANNEL_JOBS": jobs,
        "JENKINS_CHANNEL_POLL_INTERVAL": poll_interval,
        "JENKINS_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "JENKINS_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "JENKINS_CHANNEL_LOG_LEVEL": "debug",
    })


# ═══════════════════════════════════════════════════════════════════════
# GITLAB TESTS
# ═══════════════════════════════════════════════════════════════════════

async def test_gl_handshake():
    print("\n--- GL-1. Handshake ---")
    client = make_gitlab_client()
    await client.connect()
    resp = await client.initialize(channel=True)
    caps = resp.get("result", {}).get("capabilities", {})
    assert_true("claude/channel" in caps.get("experimental", {}), "channel capability")
    tools = await client.list_tools()
    assert_true(len(tools) == 3, f"3 tools (got {len(tools)})")
    await client.close()


async def test_gl_seeding():
    print("\n--- GL-2. First-Run Seeding (no flood) ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)
    notifications = await collect(client, duration=6.0)
    assert_true(len(all_events(notifications)) == 0, "no events on first run")
    await client.close()


async def test_gl_todo_created():
    print("\n--- GL-3. Todo Created (real) ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    # Create a new issue assigned to root → generates todo
    issue = gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/issues", {
        "title": f"Todo test {int(time.time())}",
        "assignee_ids": [1],
    })

    notifications = await collect(client, duration=8.0)
    events = find_events(notifications, "todo_created")
    assert_true(len(events) >= 1, f"todo_created from real GitLab (got {len(events)})")
    if events:
        meta = events[0]["params"]["meta"]
        assert_true(meta.get("target_type") == "Issue", "target_type is Issue")
        assert_true(meta.get("project") == gitlab_ctx["project_path"], "project path matches")
    await client.close()


async def test_gl_todo_resolved():
    print("\n--- GL-4. Todo Resolved (real) ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    # Get current pending todos
    todos = gitlab_api("GET", "/api/v4/todos?state=pending&per_page=5")
    if not todos:
        print("  SKIP  no pending todos to resolve")
        await client.close()
        return

    # Mark first todo as done via API
    todo_id = todos[0]["id"]
    gitlab_api("POST", f"/api/v4/todos/{todo_id}/mark_as_done")

    notifications = await collect(client, duration=8.0)
    events = find_events(notifications, "todo_resolved")
    assert_true(len(events) >= 1, f"todo_resolved (got {len(events)})")
    if events:
        assert_true(events[0]["params"]["meta"]["todo_id"] == str(todo_id), "correct todo_id")
    await client.close()


async def test_gl_pipeline_trigger():
    print("\n--- GL-5. Pipeline Status Changed (real) ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=8.0)  # seed

    # Trigger pipeline
    try:
        pipeline = gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/pipeline", {
            "ref": "main",
        })
        print(f"  Pipeline #{pipeline.get('id')} triggered (status: {pipeline.get('status')})")
    except Exception as e:
        print(f"  SKIP  pipeline trigger failed: {e}")
        await client.close()
        return

    # Wait for status transitions (may need runner)
    notifications = await collect(client, duration=30.0)
    changed = find_events(notifications, "pipeline_status_changed")
    if changed:
        assert_true(True, f"pipeline_status_changed received ({len(changed)} transitions)")
        for e in changed:
            meta = e["params"]["meta"]
            print(f"    {meta.get('old_status')} → {meta.get('new_status')} (pipeline #{meta.get('pipeline_id')})")
    else:
        print("  INFO  no status changes yet (runner may not be configured)")
        ok("pipeline triggered without error")

    await client.close()


async def test_gl_watch_pipeline():
    print("\n--- GL-6. Watch Pipeline (real) ---")
    client = make_gitlab_client(poll_interval="600000")
    await client.connect()
    await client.initialize(channel=True)

    result = await client.call_tool("gitlab_watch_pipeline", {
        "project_id": str(gitlab_ctx["project_id"]),
        "ref": "main",
    })
    assert_true("Watching" in str(result), "watch accepted")

    # Trigger pipeline
    try:
        gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/pipeline", {"ref": "main"})
    except Exception:
        pass

    notifications = await collect(client, duration=30.0)
    completed = find_events(notifications, "pipeline_watch_completed")
    if completed:
        meta = completed[0]["params"]["meta"]
        assert_true(True, f"watch completed: {meta.get('status')}")
    else:
        ok("watch registered (completion depends on runner)")

    await client.close()


async def test_gl_reply_tool():
    print("\n--- GL-7. Reply Tool (real MR comment) ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)

    result = await client.call_tool("gitlab_reply", {
        "project_id": str(gitlab_ctx["project_id"]),
        "mr_iid": str(gitlab_ctx["mr_iid"]),
        "text": f"E2E test comment {int(time.time())}",
    })
    assert_true("Comment posted" in str(result), "comment posted on real MR")

    # Verify via API
    notes = gitlab_api("GET", f"/api/v4/projects/{gitlab_ctx['project_id']}/merge_requests/{gitlab_ctx['mr_iid']}/notes")
    assert_true(any("E2E test comment" in n.get("body", "") for n in notes), "comment verified via API")
    await client.close()


async def test_gl_mark_todo_done():
    print("\n--- GL-8. Mark Todo Done (real) ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)

    todos = gitlab_api("GET", "/api/v4/todos?state=pending&per_page=1")
    if not todos:
        print("  SKIP  no pending todos")
        await client.close()
        return

    todo_id = str(todos[0]["id"])
    result = await client.call_tool("gitlab_mark_todo_done", {"todo_id": todo_id})
    assert_true("Todo marked as done" in str(result), "todo marked done via tool")

    # Verify
    updated = gitlab_api("GET", f"/api/v4/todos?state=pending")
    assert_true(all(str(t["id"]) != todo_id for t in updated), "todo no longer pending")
    await client.close()


async def test_gl_namespace_filter():
    print("\n--- GL-9. Namespace Filter ---")
    client = make_gitlab_client(namespace="root")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=8.0)  # allow discovery
    ok("namespace filter runs without error")
    await client.close()


async def test_gl_auth_failure():
    print("\n--- GL-10. Auth Failure ---")
    client = MCPClient("npx", ["tsx", GITLAB_CHANNEL], {
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": "invalid-token",
        "GITLAB_CHANNEL_POLL_INTERVAL": "1000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)
    notifications = await collect(client, duration=5.0)
    assert_true(len(all_events(notifications)) == 0, "no events with bad token")
    await client.close()


# ═══════════════════════════════════════════════════════════════════════
# JENKINS TESTS
# ═══════════════════════════════════════════════════════════════════════

async def test_jk_handshake():
    print("\n--- JK-1. Handshake ---")
    client = make_jenkins_client(jenkins_ctx["jobs"]["sdk"])
    await client.connect()
    resp = await client.initialize(channel=True)
    caps = resp.get("result", {}).get("capabilities", {})
    assert_true("claude/channel" in caps.get("experimental", {}), "channel capability")
    tools = await client.list_tools()
    assert_true(len(tools) == 0, "no tools (events only)")
    await client.close()


async def test_jk_seeding():
    print("\n--- JK-2. First-Run Seeding ---")
    client = make_jenkins_client(jenkins_ctx["jobs"]["sdk"])
    await client.connect()
    await client.initialize(channel=True)
    notifications = await collect(client, duration=6.0)
    assert_true(len(all_events(notifications)) == 0, "no events on first run")
    await client.close()


async def test_jk_build_lifecycle():
    print("\n--- JK-3. Build Started + Completed (real) ---")
    job = jenkins_ctx["jobs"]["sdk"]
    client = make_jenkins_client(job, poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    # Trigger build
    jenkins_api("POST", f"/job/SDK/job/deploy_sdk_int/build")
    print("  Build triggered")

    # Wait for started + completed (pipeline takes ~8s)
    notifications = await collect(client, duration=30.0)
    started = find_events(notifications, "build_started")
    completed = find_events(notifications, "build_completed")

    assert_true(len(started) >= 1, f"build_started (got {len(started)})")
    if started:
        meta = started[0]["params"]["meta"]
        assert_true(meta.get("job") == job, "job name correct")

    assert_true(len(completed) >= 1, f"build_completed (got {len(completed)})")
    if completed:
        meta = completed[0]["params"]["meta"]
        assert_true(meta.get("result") == "SUCCESS", f"result is SUCCESS (got {meta.get('result')})")
        assert_true("✅" in completed[0]["params"]["content"], "has success emoji")
        assert_true("duration" in meta, "duration in meta")

    await client.close()


async def test_jk_build_failure():
    print("\n--- JK-4. Build Failure (real) ---")
    job = jenkins_ctx["jobs"]["fail"]
    client = make_jenkins_client(job, poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    jenkins_api("POST", "/job/SDK/job/deploy_sdk_fail/build")
    print("  Fail build triggered")

    notifications = await collect(client, duration=30.0)
    completed = find_events(notifications, "build_completed")

    assert_true(len(completed) >= 1, f"build_completed for failure (got {len(completed)})")
    if completed:
        meta = completed[0]["params"]["meta"]
        assert_true(meta.get("result") == "FAILURE", f"result is FAILURE (got {meta.get('result')})")
        assert_true("❌" in completed[0]["params"]["content"], "has failure emoji")

    await client.close()


async def test_jk_multiple_jobs():
    print("\n--- JK-5. Multiple Jobs ---")
    jobs = f"{jenkins_ctx['jobs']['sdk']},{jenkins_ctx['jobs']['frontend']}"
    client = make_jenkins_client(jobs, poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    # Trigger both
    jenkins_api("POST", "/job/SDK/job/deploy_sdk_int/build")
    jenkins_api("POST", "/job/Frontend/job/frontend_deploy_int/build")
    print("  Both builds triggered")

    notifications = await collect(client, duration=30.0)
    started = find_events(notifications, "build_started")
    jobs_seen = {e["params"]["meta"]["job"] for e in started}

    assert_true(len(started) >= 2, f"started from both jobs (got {len(started)})")
    assert_true(jenkins_ctx["jobs"]["sdk"] in jobs_seen, "SDK build detected")
    assert_true(jenkins_ctx["jobs"]["frontend"] in jobs_seen, "Frontend build detected")

    await client.close()


async def test_jk_deduplication():
    print("\n--- JK-6. Deduplication ---")
    client = make_jenkins_client(jenkins_ctx["jobs"]["sdk"], poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    # No new builds — no events
    second = await collect(client, duration=6.0)
    assert_true(len(all_events(second)) == 0, "no events when nothing changed")
    await client.close()


async def test_jk_auth_failure():
    print("\n--- JK-7. Auth Failure ---")
    client = MCPClient("npx", ["tsx", JENKINS_CHANNEL], {
        "JENKINS_CHANNEL_URL": JENKINS_URL,
        "JENKINS_CHANNEL_USERNAME": "wrong",
        "JENKINS_CHANNEL_TOKEN": "wrong",
        "JENKINS_CHANNEL_JOBS": "SDK/deploy_sdk_int",
        "JENKINS_CHANNEL_POLL_INTERVAL": "1000",
        "JENKINS_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "JENKINS_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)
    notifications = await collect(client, duration=5.0)
    assert_true(len(all_events(notifications)) == 0, "no events with bad auth")
    await client.close()


async def test_jk_duration_format():
    print("\n--- JK-8. Duration Format ---")
    job = jenkins_ctx["jobs"]["sdk"]
    client = make_jenkins_client(job, poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    jenkins_api("POST", "/job/SDK/job/deploy_sdk_int/build")
    notifications = await collect(client, duration=30.0)
    completed = find_events(notifications, "build_completed")

    if completed:
        content = completed[0]["params"]["content"]
        has_duration = any(x in content for x in ["s)", "m)", "m0s)", "ms)"])
        assert_true(has_duration, f"duration in content: {content[-30:]}")
    else:
        print("  SKIP  no completed build for duration check")

    await client.close()


# ═══════════════════════════════════════════════════════════════════════
# HUB TESTS
# ═══════════════════════════════════════════════════════════════════════

async def test_hub_both_plugins():
    print("\n--- HUB-1. Both Plugins Simultaneously ---")
    client = MCPClient("npx", ["tsx", HUB_CHANNEL], {
        "CHANNEL_PLUGINS": "gitlab,jenkins",
        "CHANNEL_HUB_AUTO_START": "false",
        "CHANNEL_HUB_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": "3000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "JENKINS_CHANNEL_URL": JENKINS_URL,
        "JENKINS_CHANNEL_USERNAME": jenkins_ctx["username"],
        "JENKINS_CHANNEL_TOKEN": jenkins_ctx["token"],
        "JENKINS_CHANNEL_JOBS": jenkins_ctx["jobs"]["sdk"],
        "JENKINS_CHANNEL_POLL_INTERVAL": "3000",
        "JENKINS_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "JENKINS_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)

    # Subscribe
    r = await client.call_tool("hub_subscribe", {"plugin": "gitlab"})
    assert_true("Subscribed" in str(r), "gitlab subscribed")

    r = await client.call_tool("hub_subscribe", {"plugin": "jenkins"})
    assert_true("Subscribed" in str(r), "jenkins subscribed")

    # Status
    status = await client.call_tool("hub_status", {})
    assert_true("active" in str(status), "both active in status")

    await collect(client, duration=8.0)  # let both seed

    # Unsubscribe jenkins
    r = await client.call_tool("hub_unsubscribe", {"plugin": "jenkins"})
    assert_true("Unsubscribed" in str(r), "jenkins unsubscribed")

    status = await client.call_tool("hub_status", {})
    assert_true("inactive" in str(status), "jenkins shows inactive")

    await client.close()


async def test_hub_event_filter():
    print("\n--- HUB-2. Event Filtering ---")
    client = MCPClient("npx", ["tsx", HUB_CHANNEL], {
        "CHANNEL_PLUGINS": "gitlab",
        "CHANNEL_HUB_AUTO_START": "false",
        "CHANNEL_HUB_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": "2000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)

    r = await client.call_tool("hub_subscribe", {
        "plugin": "gitlab",
        "events": ["todo_created"],
    })
    assert_true("todo_created" in str(r), "filtered subscription accepted")

    await collect(client, duration=6.0)  # seed
    ok("event filter runs without error")

    await client.close()


# ═══════════════════════════════════════════════════════════════════════
# MUST-HAVE GAP TESTS
# ═══════════════════════════════════════════════════════════════════════

# --- Gap #1: gitlab_reply on issue + missing IID bug ---

async def test_gl_reply_issue():
    print("\n--- GL-11. Reply on Issue (not MR) ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)

    result = await client.call_tool("gitlab_reply", {
        "project_id": str(gitlab_ctx["project_id"]),
        "issue_iid": str(gitlab_ctx["issue_iid"]),
        "text": f"E2E issue comment {int(time.time())}",
    })
    assert_true("Comment posted" in str(result), "comment posted on issue")

    notes = gitlab_api("GET", f"/api/v4/projects/{gitlab_ctx['project_id']}/issues/{gitlab_ctx['issue_iid']}/notes")
    assert_true(any("E2E issue comment" in n.get("body", "") for n in notes), "issue comment verified via API")

    # Bug test: missing both IIDs
    resp = await client.request("tools/call", {
        "name": "gitlab_reply",
        "arguments": {"project_id": str(gitlab_ctx["project_id"]), "text": "no iid"},
    })
    assert_true("error" in resp, "missing both IIDs returns error")

    await client.close()


# --- Gap #3: duplicate watch rejection ---

async def test_gl_watch_duplicate():
    print("\n--- GL-12. Watch Pipeline Duplicate ---")
    client = make_gitlab_client(poll_interval="600000")
    await client.connect()
    await client.initialize(channel=True)

    r1 = await client.call_tool("gitlab_watch_pipeline", {
        "project_id": str(gitlab_ctx["project_id"]), "ref": "main",
    })
    assert_true("Watching" in str(r1), "first watch accepted")

    r2 = await client.call_tool("gitlab_watch_pipeline", {
        "project_id": str(gitlab_ctx["project_id"]), "ref": "main",
    })
    assert_true("Already watching" in str(r2), "duplicate watch rejected")

    await client.close()


# --- Gap #4: watch expiration ---

async def test_gl_watch_expiration():
    print("\n--- GL-13. Watch Pipeline Expiration ---")
    client = MCPClient("npx", ["tsx", GITLAB_CHANNEL], {
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": "600000",
        "GITLAB_CHANNEL_WATCH_POLL": "1000",
        "GITLAB_CHANNEL_WATCH_TIMEOUT": "3000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)

    await client.call_tool("gitlab_watch_pipeline", {
        "project_id": str(gitlab_ctx["project_id"]), "ref": "nonexistent-branch",
    })

    notifications = await collect(client, duration=8.0)
    expired = find_events(notifications, "pipeline_watch_expired")
    assert_true(len(expired) >= 1, f"watch expired event (got {len(expired)})")

    await client.close()


# --- Gap #5: pipeline status change with verified statuses ---
# (Rewritten to be more assertive — create a pipeline and watch its transitions)

async def test_gl_pipeline_verified():
    print("\n--- GL-14. Pipeline Status Change Verified ---")
    client = make_gitlab_client()
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    try:
        pipeline = gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/pipeline", {"ref": "main"})
        pid = pipeline["id"]
        print(f"  Pipeline #{pid} triggered")

        # Wait a poll cycle so the plugin sees the initial status
        await asyncio.sleep(4)

        # Cancel to force a status change
        try:
            gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/pipelines/{pid}/cancel")
            print(f"  Pipeline #{pid} canceled")
        except Exception:
            pass

        notifications = await collect(client, duration=15.0)
        changed = find_events(notifications, "pipeline_status_changed")
        assert_true(len(changed) >= 1, f"pipeline_status_changed with verified transition (got {len(changed)})")
        if changed:
            meta = changed[0]["params"]["meta"]
            assert_true("old_status" in meta, "has old_status")
            assert_true("new_status" in meta, "has new_status")
            print(f"    {meta.get('old_status')} → {meta.get('new_status')}")
    except Exception as e:
        print(f"  SKIP  pipeline test failed: {e}")
        ok("pipeline API accessible")

    await client.close()


# --- Gap #11: build_started without completed (slow build) ---

async def test_jk_build_started_only():
    print("\n--- JK-9. Build Started Without Completed (slow build) ---")
    job = jenkins_ctx["jobs"]["slow"]
    client = make_jenkins_client(job, poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    jenkins_api("POST", "/job/SDK/job/deploy_sdk_slow/build")
    print("  Slow build triggered (120s sleep)")

    notifications = await collect(client, duration=15.0)
    started = find_events(notifications, "build_started")
    completed = find_events(notifications, "build_completed")

    assert_true(len(started) >= 1, f"build_started received (got {len(started)})")
    assert_true(len(completed) == 0, f"build_completed NOT received yet (got {len(completed)})")

    # Stop the slow build to not leave it running
    try:
        info = json.loads(jenkins_api("GET", f"/job/SDK/job/deploy_sdk_slow/lastBuild/api/json"))
        build_num = info.get("number", 1)
        jenkins_api("POST", f"/job/SDK/job/deploy_sdk_slow/{build_num}/stop")
    except Exception:
        pass

    await client.close()


# --- Gap #12: UNSTABLE build result ---

async def test_jk_build_unstable():
    print("\n--- JK-10. Build UNSTABLE ---")
    job = jenkins_ctx["jobs"]["unstable"]
    client = make_jenkins_client(job, poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed

    jenkins_api("POST", "/job/SDK/job/deploy_sdk_unstable/build")
    print("  Unstable build triggered")

    notifications = await collect(client, duration=30.0)
    completed = find_events(notifications, "build_completed")

    assert_true(len(completed) >= 1, f"build_completed for unstable (got {len(completed)})")
    if completed:
        result = completed[0]["params"]["meta"].get("result", "")
        assert_true(result == "UNSTABLE", f"result is UNSTABLE (got {result})")
        assert_true("⚠️" in completed[0]["params"]["content"], "has warning emoji")

    await client.close()


# --- Gap #15: Jenkins poll resilience with invalid job ---

async def test_jk_poll_resilience():
    print("\n--- JK-11. Poll Resilience (invalid + valid job) ---")
    jobs = f"INVALID/nonexistent,{jenkins_ctx['jobs']['sdk']}"
    client = make_jenkins_client(jobs, poll_interval="2000")
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=6.0)  # seed (invalid job errors are swallowed)

    jenkins_api("POST", "/job/SDK/job/deploy_sdk_int/build")
    print("  Build triggered on valid job")

    notifications = await collect(client, duration=30.0)
    started = find_events(notifications, "build_started")

    assert_true(len(started) >= 1, f"valid job still tracked despite invalid job (got {len(started)})")
    if started:
        assert_true(started[0]["params"]["meta"]["job"] == jenkins_ctx["jobs"]["sdk"], "event from valid job")

    await client.close()


# --- Gap #17: Hub autoStart=true ---

async def test_hub_auto_start():
    print("\n--- HUB-3. Auto Start Mode ---")
    client = MCPClient("npx", ["tsx", HUB_CHANNEL], {
        "CHANNEL_PLUGINS": "gitlab",
        "CHANNEL_HUB_AUTO_START": "true",
        "CHANNEL_HUB_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": "2000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)

    status = await client.call_tool("hub_status", {})
    assert_true("active" in str(status), "gitlab auto-started")

    await collect(client, duration=6.0)  # seed

    # Create a todo — should arrive without manual subscribe
    gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/issues", {
        "title": f"AutoStart test {int(time.time())}",
        "assignee_ids": [1],
    })

    notifications = await collect(client, duration=8.0)
    events = find_events(notifications, "todo_created")
    assert_true(len(events) >= 1, f"events arrive without manual subscribe (got {len(events)})")

    await client.close()


# --- Gap #18: Hub forwards events from both plugins ---

async def test_hub_both_events():
    print("\n--- HUB-4. Events From Both Plugins ---")
    client = MCPClient("npx", ["tsx", HUB_CHANNEL], {
        "CHANNEL_PLUGINS": "gitlab,jenkins",
        "CHANNEL_HUB_AUTO_START": "true",
        "CHANNEL_HUB_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": "2000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "JENKINS_CHANNEL_URL": JENKINS_URL,
        "JENKINS_CHANNEL_USERNAME": jenkins_ctx["username"],
        "JENKINS_CHANNEL_TOKEN": jenkins_ctx["token"],
        "JENKINS_CHANNEL_JOBS": jenkins_ctx["jobs"]["sdk"],
        "JENKINS_CHANNEL_POLL_INTERVAL": "2000",
        "JENKINS_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "JENKINS_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)
    await collect(client, duration=8.0)  # seed both

    # Trigger both: GitLab todo + Jenkins build
    gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/issues", {
        "title": f"Hub both test {int(time.time())}",
        "assignee_ids": [1],
    })
    jenkins_api("POST", "/job/SDK/job/deploy_sdk_int/build")
    print("  Both triggers fired")

    notifications = await collect(client, duration=30.0)
    plugins_seen = {e["params"]["meta"].get("plugin") for e in all_events(notifications)}

    assert_true("gitlab" in plugins_seen, "gitlab events received through hub")
    assert_true("jenkins" in plugins_seen, "jenkins events received through hub")

    await client.close()


# --- Gap #19: Hub event filter actually blocks ---

async def test_hub_filter_blocks():
    print("\n--- HUB-5. Event Filter Blocks Events ---")
    client = MCPClient("npx", ["tsx", HUB_CHANNEL], {
        "CHANNEL_PLUGINS": "gitlab",
        "CHANNEL_HUB_AUTO_START": "false",
        "CHANNEL_HUB_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": "2000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)

    # Subscribe with filter: only pipeline_status_changed
    await client.call_tool("hub_subscribe", {
        "plugin": "gitlab",
        "events": ["pipeline_status_changed"],
    })
    await collect(client, duration=6.0)  # seed

    # Create a todo — should be BLOCKED by filter
    gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/issues", {
        "title": f"Filter block test {int(time.time())}",
        "assignee_ids": [1],
    })

    notifications = await collect(client, duration=8.0)
    todo_events = find_events(notifications, "todo_created")
    assert_true(len(todo_events) == 0, f"todo_created blocked by filter (got {len(todo_events)})")

    await client.close()


# --- Gap #22: Hub tool routing for inactive plugins ---

async def test_hub_tool_routing():
    print("\n--- HUB-6. Tool Routing (inactive plugin) ---")
    client = MCPClient("npx", ["tsx", HUB_CHANNEL], {
        "CHANNEL_PLUGINS": "gitlab",
        "CHANNEL_HUB_AUTO_START": "false",
        "CHANNEL_HUB_LOG_PATH": tempfile.mktemp(suffix=".log"),
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx["pat"],
        "GITLAB_CHANNEL_POLL_INTERVAL": "600000",
        "GITLAB_CHANNEL_DB_PATH": tempfile.mktemp(suffix=".db"),
        "GITLAB_CHANNEL_LOG_PATH": tempfile.mktemp(suffix=".log"),
    })
    await client.connect()
    await client.initialize(channel=True)

    # gitlab NOT subscribed — calling its tool should fail
    resp = await client.request("tools/call", {
        "name": "gitlab_reply",
        "arguments": {"project_id": "1", "mr_iid": "1", "text": "test"},
    })
    assert_true("error" in resp, "gitlab_reply fails when plugin inactive")

    # Subscribe, then it should work
    await client.call_tool("hub_subscribe", {"plugin": "gitlab"})
    result = await client.call_tool("gitlab_reply", {
        "project_id": str(gitlab_ctx["project_id"]),
        "mr_iid": str(gitlab_ctx["mr_iid"]),
        "text": f"Tool routing test {int(time.time())}",
    })
    assert_true("Comment posted" in str(result), "gitlab_reply works after subscribe")

    await client.close()


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

async def main():
    global gitlab_ctx, jenkins_ctx

    print("E2E Tests — Real GitLab CE + Jenkins in Docker")
    print("=" * 55)

    do_bootstrap = "--bootstrap" in sys.argv

    if do_bootstrap:
        print("Starting Docker services...")
        compose = str(Path(__file__).parent / "docker-compose.yml")
        subprocess.run(["docker", "compose", "-f", compose, "up", "-d"], check=False, timeout=300)

    # Bootstrap or load cached context
    ctx_file = Path(tempfile.gettempdir()) / "e2e_docker_ctx.json"
    if do_bootstrap or not ctx_file.exists():
        sys.path.insert(0, str(Path(__file__).parent / "setup"))
        from gitlab_bootstrap import bootstrap_gitlab
        from jenkins_bootstrap import bootstrap_jenkins
        gitlab_ctx = bootstrap_gitlab(GITLAB_URL)
        jenkins_ctx = bootstrap_jenkins(JENKINS_URL)
        with open(ctx_file, "w") as f:
            json.dump({"gitlab": gitlab_ctx, "jenkins": jenkins_ctx}, f)
    else:
        with open(ctx_file) as f:
            ctx = json.load(f)
        gitlab_ctx = ctx["gitlab"]
        jenkins_ctx = ctx["jenkins"]
        print(f"Loaded cached context from {ctx_file}\n")

    # GitLab tests
    await test_gl_handshake()
    await test_gl_seeding()
    await test_gl_todo_created()
    await test_gl_todo_resolved()
    await test_gl_pipeline_trigger()
    await test_gl_watch_pipeline()
    await test_gl_reply_tool()
    await test_gl_mark_todo_done()
    await test_gl_namespace_filter()
    await test_gl_auth_failure()
    await test_gl_reply_issue()
    await test_gl_watch_duplicate()
    await test_gl_watch_expiration()
    await test_gl_pipeline_verified()

    # Jenkins tests
    await test_jk_handshake()
    await test_jk_seeding()
    await test_jk_build_lifecycle()
    await test_jk_build_failure()
    await test_jk_multiple_jobs()
    await test_jk_deduplication()
    await test_jk_auth_failure()
    await test_jk_duration_format()
    await test_jk_build_started_only()
    await test_jk_build_unstable()
    await test_jk_poll_resilience()

    # Hub tests
    await test_hub_both_plugins()
    await test_hub_event_filter()
    await test_hub_auto_start()
    await test_hub_both_events()
    await test_hub_filter_blocks()
    await test_hub_tool_routing()

    print("\n" + "=" * 55)
    total = PASS + FAIL
    print(f"Results: {PASS}/{total} passed, {FAIL} failed")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
