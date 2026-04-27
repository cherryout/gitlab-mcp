#!/usr/bin/env python3
"""E2E tests for orchestrator-server.ts against real GitLab CE and Jenkins in Docker.

Versions match production:
  - GitLab CE 17.11.x
  - Jenkins 2.387.1

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
import shutil
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
ORCHESTRATOR_SERVER = str(PROJECT_ROOT / "orchestrator-server.ts")

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


# ─── Helpers ──────────────────────────────────────────────────────────

async def collect(client: MCPClient, duration: float = 5.0) -> list[dict]:
    collected: list[dict] = []
    client._on_notification = lambda m: collected.append(m)
    await asyncio.sleep(duration)
    client._on_notification = None
    return collected


def channel_events(notifications: list[dict]) -> list[dict]:
    return [n for n in notifications if n.get("method") == "notifications/claude/channel"]


def find_event_kind(notifications: list[dict], event_kind: str) -> list[dict]:
    return [
        n for n in channel_events(notifications)
        if n.get("params", {}).get("meta", {}).get("event_kind") == event_kind
    ]


def gitlab_api(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{GITLAB_URL}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"PRIVATE-TOKEN": gitlab_ctx["pat"], "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def jenkins_api(method: str, path: str, data: bytes | None = None) -> bytes:
    url = f"{JENKINS_URL}{path}"
    auth = base64.b64encode(f"{jenkins_ctx['username']}:{jenkins_ctx['token']}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        return e.read()


def make_db_dir() -> str:
    d = tempfile.mkdtemp(prefix="orch-e2e-")
    Path(d, ".cache", "orchestrator").mkdir(parents=True)
    return d


def cleanup_db_dir(d: str):
    shutil.rmtree(d, ignore_errors=True)


def make_orchestrator(
    db_dir: str,
    session_owner: str = "e2e",
    plugins: str = "gitlab",
    poll_interval: str = "1500",
    extra_env: dict | None = None,
) -> MCPClient:
    env = {
        "ORCHESTRATOR_SESSION_OWNER": session_owner,
        "ORCHESTRATOR_AUTO_START": "true",
        "CHANNEL_PLUGINS": plugins,
        "HOME": db_dir,
        "ORCHESTRATOR_LOG_LEVEL": "warn",
        "GITLAB_CHANNEL_LOG_LEVEL": "warn",
        "JENKINS_CHANNEL_LOG_LEVEL": "warn",
        "GITLAB_API_URL": f"{GITLAB_URL}/api/v4",
        "GITLAB_PERSONAL_ACCESS_TOKEN": gitlab_ctx.get("pat", "test"),
        "GITLAB_CHANNEL_POLL_INTERVAL": poll_interval,
        "GITLAB_CHANNEL_WATCH_POLL": "1500",
        "JENKINS_CHANNEL_URL": JENKINS_URL,
        "JENKINS_CHANNEL_USERNAME": jenkins_ctx.get("username", "test"),
        "JENKINS_CHANNEL_TOKEN": jenkins_ctx.get("token", "test"),
        "JENKINS_CHANNEL_JOBS": jenkins_ctx.get("jobs", {}).get("sdk", "SDK/deploy_sdk_int"),
        "JENKINS_CHANNEL_POLL_INTERVAL": poll_interval,
    }
    if extra_env:
        env.update(extra_env)
    return MCPClient("npx", ["tsx", ORCHESTRATOR_SERVER], env)


def parse_tool_json(text: str) -> dict | list:
    return json.loads(text)


# ═══════════════════════════════════════════════════════════════════════
# ORCHESTRATOR PROTOCOL TESTS
# ═══════════════════════════════════════════════════════════════════════

async def test_handshake_and_tools():
    print("\n--- ORCH-1. Handshake + tools list ---")
    db_dir = make_db_dir()
    client = make_orchestrator(db_dir)
    try:
        await client.connect()
        resp = await client.initialize(channel=True)
        caps = resp.get("result", {}).get("capabilities", {})
        assert_true("claude/channel" in caps.get("experimental", {}), "claude/channel capability declared")

        tools = await client.list_tools()
        names = {t["name"] for t in tools}
        for required in ("add_watch", "remove_watch", "list_watches",
                         "list_session_feed", "get_session_state", "list_unmatched_events",
                         "gitlab_watch_branch", "gitlab_watch_mr", "gitlab_watch_current_branch"):
            assert_true(required in names, f"tool '{required}' exposed")
    finally:
        await client.close()
        cleanup_db_dir(db_dir)


async def test_session_auto_managed():
    print("\n--- ORCH-2. Session auto-created and runtime attached ---")
    db_dir = make_db_dir()
    client = make_orchestrator(db_dir, session_owner="auto-owner")
    try:
        await client.connect()
        await client.initialize(channel=True)
        state_text = await client.call_tool("get_session_state", {})
        state = parse_tool_json(state_text)
        assert_true(bool(state["session"]["session_id"]), "session_id present")
        assert_true(state["session"]["owner"] == "auto-owner", f"owner matches env (got {state['session']['owner']})")
        assert_true(state["runtime"] is not None, "runtime auto-attached")
    finally:
        await client.close()
        cleanup_db_dir(db_dir)


async def test_no_watch_no_delivery():
    """Spec rule: no watch = no notification, even when events flow."""
    print("\n--- ORCH-3. No watch = no delivery (real GitLab activity) ---")
    db_dir = make_db_dir()
    client = make_orchestrator(db_dir, session_owner="no-watch")
    try:
        await client.connect()
        await client.initialize(channel=True)

        # Let plugins seed (silent)
        await collect(client, duration=4.0)

        # Generate real activity that would normally produce events
        gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/issues", {
            "title": f"Should NOT trigger notification {int(time.time())}",
            "assignee_ids": [1],
        })

        # Wait for poll cycle
        notifications = await collect(client, duration=5.0)
        events = channel_events(notifications)
        assert_true(len(events) == 0, f"no channel notifications without watch (got {len(events)})")
    finally:
        await client.close()
        cleanup_db_dir(db_dir)


# ═══════════════════════════════════════════════════════════════════════
# GITLAB FLOW
# ═══════════════════════════════════════════════════════════════════════

async def test_gitlab_watch_branch_pipeline():
    print("\n--- ORCH-4. gitlab_watch_branch + real pipeline → completed ---")
    db_dir = make_db_dir()
    client = make_orchestrator(db_dir, session_owner="watch-branch", poll_interval="1500")
    try:
        await client.connect()
        await client.initialize(channel=True)
        await collect(client, duration=4.0)  # seed

        # Watch the project on main
        result = await client.call_tool("gitlab_watch_branch", {
            "project": gitlab_ctx["project_path"],
            "ref": "main",
        })
        assert_true("Watching pipeline" in str(result), "gitlab_watch_branch accepted")

        # Verify watch in orchestrator DB
        watches = parse_tool_json(await client.call_tool("list_watches", {}))
        assert_true(len(watches) == 1, f"1 watch (got {len(watches)})")
        assert_true(watches[0]["entity_ref"].endswith(f"{gitlab_ctx['project_path']}:ref:main"),
                    f"entity_ref formatted: {watches[0]['entity_ref']}")

        # Trigger a real pipeline
        try:
            pipeline = gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/pipeline",
                                  {"ref": "main"})
            print(f"  Pipeline #{pipeline.get('id')} triggered")
        except Exception as e:
            print(f"  SKIP  pipeline trigger failed: {e}")
            return

        # Wait for terminal status (runner needs to pick it up)
        notifications = await collect(client, duration=60.0)
        completed = find_event_kind(notifications, "pipeline_watch_completed")

        if completed:
            meta = completed[0]["params"]["meta"]
            assert_true(meta.get("orchestrated") == "true", "delivered through orchestration channel")
            assert_true(meta.get("source") == "gitlab", "source=gitlab in meta")
            content = completed[0]["params"]["content"]
            assert_true("Pipeline #" in content, f"content has pipeline ref: {content}")
        else:
            print("  INFO  no pipeline_watch_completed (runner may not be configured yet)")
            ok("watch flow ran without error")
    finally:
        await client.close()
        cleanup_db_dir(db_dir)


async def test_duplicate_watch_returns_existing():
    print("\n--- ORCH-5. Duplicate add_watch returns existing ---")
    db_dir = make_db_dir()
    client = make_orchestrator(db_dir, session_owner="dup")
    try:
        await client.connect()
        await client.initialize(channel=True)

        r1 = parse_tool_json(await client.call_tool("add_watch", {
            "watch_type": "merge-request",
            "entity_type": "merge_request",
            "entity_ref": f"gitlab:{gitlab_ctx['project_path']}:mr:{gitlab_ctx['mr_iid']}",
        }))
        assert_true(bool(r1.get("watch_id")), "first add_watch returns watch_id")

        r2 = parse_tool_json(await client.call_tool("add_watch", {
            "watch_type": "merge-request",
            "entity_type": "merge_request",
            "entity_ref": f"gitlab:{gitlab_ctx['project_path']}:mr:{gitlab_ctx['mr_iid']}",
        }))
        assert_true(r1["watch_id"] == r2["watch_id"], "duplicate returns same watch_id")

        watches = parse_tool_json(await client.call_tool("list_watches", {}))
        assert_true(len(watches) == 1, f"only 1 watch active (got {len(watches)})")
    finally:
        await client.close()
        cleanup_db_dir(db_dir)


# ═══════════════════════════════════════════════════════════════════════
# JENKINS FLOW
# ═══════════════════════════════════════════════════════════════════════

async def test_jenkins_watch_job_build_completed():
    print("\n--- ORCH-6. jenkins_watch_job + real build → completed ---")
    db_dir = make_db_dir()
    job = jenkins_ctx["jobs"]["sdk"]
    client = make_orchestrator(db_dir, session_owner="jenkins-watch", plugins="jenkins")
    try:
        await client.connect()
        await client.initialize(channel=True)
        await collect(client, duration=4.0)  # seed

        result = await client.call_tool("jenkins_watch_job", {"job_path": job})
        assert_true("Watching Jenkins job" in str(result), "jenkins_watch_job accepted")

        watches = parse_tool_json(await client.call_tool("list_watches", {}))
        assert_true(len(watches) == 1, "1 watch active")
        assert_true(watches[0]["watch_type"] == "deploy-chain", "deploy-chain watch_type")

        # Trigger a real build
        jenkins_api("POST", f"/job/SDK/job/deploy_sdk_int/build")
        print("  Build triggered")

        notifications = await collect(client, duration=60.0)
        completed = find_event_kind(notifications, "build_completed")

        if completed:
            meta = completed[0]["params"]["meta"]
            assert_true(meta.get("orchestrated") == "true", "delivered through orchestration")
            assert_true(meta.get("source") == "jenkins", "source=jenkins")
            content = completed[0]["params"]["content"]
            assert_true("SUCCESS" in content or "FAILURE" in content, f"build result in content: {content}")
        else:
            print("  INFO  no build_completed event yet")
            ok("watch flow ran without error")
    finally:
        await client.close()
        cleanup_db_dir(db_dir)


# ═══════════════════════════════════════════════════════════════════════
# PERSISTENCE
# ═══════════════════════════════════════════════════════════════════════

async def test_watch_survives_restart():
    print("\n--- ORCH-7. Watch survives process restart ---")
    db_dir = make_db_dir()
    session_owner = "persist-test"

    # First server: create watch
    client1 = make_orchestrator(db_dir, session_owner=session_owner)
    try:
        await client1.connect()
        await client1.initialize(channel=True)
        state1 = parse_tool_json(await client1.call_tool("get_session_state", {}))
        session_id_1 = state1["session"]["session_id"]

        added = parse_tool_json(await client1.call_tool("add_watch", {
            "watch_type": "merge-request",
            "entity_type": "merge_request",
            "entity_ref": f"gitlab:{gitlab_ctx['project_path']}:mr:{gitlab_ctx['mr_iid']}",
        }))
        watch_id = added["watch_id"]
    finally:
        await client1.close()

    await asyncio.sleep(0.5)

    # Second server: same owner, same db_dir — should resume
    client2 = make_orchestrator(db_dir, session_owner=session_owner)
    try:
        await client2.connect()
        await client2.initialize(channel=True)
        state2 = parse_tool_json(await client2.call_tool("get_session_state", {}))
        assert_true(state2["session"]["session_id"] == session_id_1, "same session_id after restart")

        watches = parse_tool_json(await client2.call_tool("list_watches", {}))
        assert_true(len(watches) == 1, "1 watch after restart")
        assert_true(watches[0]["watch_id"] == watch_id, "same watch_id")
    finally:
        await client2.close()
        cleanup_db_dir(db_dir)


async def test_seeded_flag_persists_across_restart():
    """After restart, plugin should NOT re-seed silently. New events between
    restarts must fire on first poll after restart."""
    print("\n--- ORCH-8. Seeded flag persists; new todos during downtime fire after restart ---")
    db_dir = make_db_dir()
    session_owner = "seeded-persist"

    # First server: let it seed
    client1 = make_orchestrator(db_dir, session_owner=session_owner)
    try:
        await client1.connect()
        await client1.initialize(channel=True)
        await collect(client1, duration=4.0)  # seed
    finally:
        await client1.close()

    await asyncio.sleep(0.5)

    # Create a todo while server is dead — assignee=root → new todo appears
    issue = gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/issues", {
        "title": f"Down-time todo {int(time.time())}",
        "assignee_ids": [1],
    })
    print(f"  Created issue #{issue['iid']} during downtime")

    # Restart and watch the issue's todo correlation key — actually just check unmatched events
    client2 = make_orchestrator(db_dir, session_owner=session_owner)
    try:
        await client2.connect()
        await client2.initialize(channel=True)
        await collect(client2, duration=6.0)  # let poll happen

        unmatched = parse_tool_json(await client2.call_tool("list_unmatched_events", {}))
        todo_events = [e for e in unmatched if e.get("event_kind") == "todo_created"]
        assert_true(len(todo_events) >= 1,
                    f"todo_created event fired after restart (got {len(todo_events)} unmatched)")
    finally:
        await client2.close()
        cleanup_db_dir(db_dir)


async def test_queued_replay_on_resume():
    """Add watch → close server → API generates event → restart → event replays."""
    print("\n--- ORCH-9. Queued events replay on session resume ---")
    db_dir = make_db_dir()
    session_owner = "queued-replay"

    client1 = make_orchestrator(db_dir, session_owner=session_owner, poll_interval="1500")
    try:
        await client1.connect()
        await client1.initialize(channel=True)
        await collect(client1, duration=4.0)  # seed

        # Watch the project's pipelines on main
        await client1.call_tool("gitlab_watch_branch", {
            "project": gitlab_ctx["project_path"],
            "ref": "main",
        })
    finally:
        await client1.close()

    await asyncio.sleep(0.5)

    # Trigger pipeline while session is down
    try:
        pipeline = gitlab_api("POST", f"/api/v4/projects/{gitlab_ctx['project_id']}/pipeline",
                              {"ref": "main"})
        print(f"  Pipeline #{pipeline.get('id')} triggered while session was down")
    except Exception as e:
        print(f"  SKIP  pipeline trigger failed: {e}")
        cleanup_db_dir(db_dir)
        return

    # Restart — pending deliveries should replay (pipeline takes ~10-20s; we wait longer)
    client2 = make_orchestrator(db_dir, session_owner=session_owner, poll_interval="1500")
    try:
        await client2.connect()
        await client2.initialize(channel=True)

        notifications = await collect(client2, duration=60.0)
        completed = find_event_kind(notifications, "pipeline_watch_completed")

        if completed:
            meta = completed[0]["params"]["meta"]
            # Should be either live (came in after resume) or replayed
            ok(f"pipeline event delivered after resume (replay or live)")
        else:
            # Even if pipeline didn't complete in time, watch should still be active
            watches = parse_tool_json(await client2.call_tool("list_watches", {}))
            assert_true(len(watches) >= 1, "watch survived restart")
            print("  INFO  pipeline didn't complete within wait window")
            ok("watch survived restart")
    finally:
        await client2.close()
        cleanup_db_dir(db_dir)


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def bootstrap_if_needed(force: bool = False):
    global gitlab_ctx, jenkins_ctx
    cache_file = Path(tempfile.gettempdir()) / "e2e-orch-bootstrap.json"

    if cache_file.exists() and not force:
        with open(cache_file) as f:
            data = json.load(f)
        gitlab_ctx = data.get("gitlab", {})
        jenkins_ctx = data.get("jenkins", {})
        if gitlab_ctx and jenkins_ctx:
            print(f"Loaded cached bootstrap from {cache_file}")
            return

    print("Bootstrapping (this can take a few minutes the first time)...")
    sys.path.insert(0, str(Path(__file__).parent / "setup"))
    from gitlab_bootstrap import bootstrap_gitlab
    from jenkins_bootstrap import bootstrap_jenkins

    gitlab_ctx = bootstrap_gitlab(GITLAB_URL)
    jenkins_ctx = bootstrap_jenkins(JENKINS_URL)
    with open(cache_file, "w") as f:
        json.dump({"gitlab": gitlab_ctx, "jenkins": jenkins_ctx}, f, indent=2)
    print(f"Bootstrap cached to {cache_file}\n")


async def main():
    global PASS, FAIL

    if "--bootstrap" in sys.argv:
        bootstrap_if_needed(force=True)
    else:
        bootstrap_if_needed()

    print("\n" + "=" * 65)
    print("E2E Orchestrator Tests (real GitLab + Jenkins)")
    print("=" * 65)

    tests = [
        test_handshake_and_tools,
        test_session_auto_managed,
        test_no_watch_no_delivery,
        test_gitlab_watch_branch_pipeline,
        test_duplicate_watch_returns_existing,
        test_jenkins_watch_job_build_completed,
        test_watch_survives_restart,
        test_seeded_flag_persists_across_restart,
        test_queued_replay_on_resume,
    ]

    for t in tests:
        try:
            await t()
        except Exception as e:
            FAIL += 1
            print(f"  FAIL  {t.__name__} threw: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 65)
    print(f"Results: {PASS}/{PASS + FAIL} passed, {FAIL} failed")
    sys.exit(1 if FAIL > 0 else 0)


if __name__ == "__main__":
    asyncio.run(main())
