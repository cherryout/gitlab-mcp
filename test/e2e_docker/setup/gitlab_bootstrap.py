"""Bootstrap GitLab CE for E2E tests (idempotent).

Creates: PAT, test projects, branches, MRs, issues, .gitlab-ci.yml.
Registers a shell runner for pipeline execution.
"""

import json
import subprocess
import time
import urllib.request
import urllib.error


def api(base_url: str, method: str, path: str, token: str, body: dict | None = None, ignore_codes: tuple = ()) -> dict | list:
    url = f"{base_url}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"PRIVATE-TOKEN": token, "Content-Type": "application/json"}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        if e.code in ignore_codes:
            return json.loads(error_body) if error_body.startswith("{") else {}
        raise RuntimeError(f"{method} {path} -> {e.code}: {error_body}") from e


def wait_for_gitlab(base_url: str, timeout: int = 420):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{base_url}/api/v4/version", method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    return
        except urllib.error.HTTPError as e:
            if e.code == 401:
                return
        except Exception:
            pass
        time.sleep(10)
    raise TimeoutError(f"GitLab not ready after {timeout}s")


def create_pat(base_url: str) -> str:
    token_data = "grant_type=password&username=root&password=e2e-test-password-123".encode()
    req = urllib.request.Request(
        f"{base_url}/oauth/token",
        data=token_data, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        oauth_token = json.loads(resp.read())["access_token"]

    body = json.dumps({
        "name": f"e2e-test-{int(time.time())}",
        "scopes": ["api", "read_user", "read_api", "write_repository"],
        "expires_at": "2027-04-01",
    }).encode()
    req = urllib.request.Request(
        f"{base_url}/api/v4/users/1/personal_access_tokens",
        data=body, method="POST",
        headers={"Authorization": f"Bearer {oauth_token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["token"]


def find_or_create_project(base_url: str, pat: str, name: str) -> dict:
    projects = api(base_url, "GET", f"/api/v4/projects?search={name}&owned=true", pat)
    existing = [p for p in projects if p["name"] == name]
    if existing:
        return existing[0]
    return api(base_url, "POST", "/api/v4/projects", pat, {
        "name": name, "visibility": "private", "initialize_with_readme": True,
    })


def register_runner(base_url: str, token: str):
    runners_token = api(base_url, "POST", "/api/v4/user/runners", token, {
        "runner_type": "instance_type", "tag_list": "e2e,shell",
    })
    runner_token = runners_token.get("token", "")
    if not runner_token:
        print(f"  Warning: could not create runner, response: {runners_token}")
        return
    try:
        subprocess.run([
            "docker", "compose", "-f", "test/e2e_docker/docker-compose.yml",
            "exec", "-T", "gitlab-runner",
            "gitlab-runner", "register",
            "--non-interactive", "--url", "http://gitlab.local",
            "--token", runner_token, "--executor", "shell", "--tag-list", "e2e,shell",
        ], check=True, timeout=30, capture_output=True)
        print("  Runner registered")
    except Exception as e:
        print(f"  Warning: runner registration failed: {e}")


def bootstrap_gitlab(base_url: str) -> dict:
    print("Bootstrapping GitLab...")

    wait_for_gitlab(base_url)
    print("  GitLab ready")

    pat = create_pat(base_url)
    print("  PAT created")

    project = find_or_create_project(base_url, pat, "e2e-test-project")
    project_id = project["id"]
    project_path = project["path_with_namespace"]
    print(f"  Project 1: {project_path} (id={project_id})")

    ci_yaml = (
        "stages:\n  - test\n\n"
        "test_pass:\n  stage: test\n  tags: [e2e]\n"
        "  script:\n    - echo 'passed'\n\n"
        "test_fail:\n  stage: test\n  tags: [e2e]\n"
        "  script:\n    - exit 1\n  when: manual\n"
    )
    try:
        api(base_url, "POST", f"/api/v4/projects/{project_id}/repository/files/.gitlab-ci.yml", pat, {
            "branch": "main", "content": ci_yaml, "commit_message": "Add CI config",
        })
    except RuntimeError:
        api(base_url, "PUT", f"/api/v4/projects/{project_id}/repository/files/.gitlab-ci.yml", pat, {
            "branch": "main", "content": ci_yaml, "commit_message": "Update CI config",
        }, ignore_codes=(400,))
    print("  .gitlab-ci.yml ready")

    api(base_url, "POST", f"/api/v4/projects/{project_id}/repository/branches", pat, {
        "branch": "feature-e2e", "ref": "main",
    }, ignore_codes=(400,))
    api(base_url, "POST", f"/api/v4/projects/{project_id}/repository/files/test.txt", pat, {
        "branch": "feature-e2e", "content": f"e2e {int(time.time())}", "commit_message": "Update test",
    }, ignore_codes=(400,))

    mrs = api(base_url, "GET", f"/api/v4/projects/{project_id}/merge_requests?state=opened", pat)
    if mrs:
        mr_iid = mrs[0]["iid"]
    else:
        mr = api(base_url, "POST", f"/api/v4/projects/{project_id}/merge_requests", pat, {
            "source_branch": "feature-e2e", "target_branch": "main", "title": "E2E test MR",
        })
        mr_iid = mr["iid"]
    print(f"  MR !{mr_iid}")

    issue = api(base_url, "POST", f"/api/v4/projects/{project_id}/issues", pat, {
        "title": "E2E test issue", "assignee_ids": [1],
    })
    print(f"  Issue #{issue['iid']}")

    project2 = find_or_create_project(base_url, pat, "e2e-test-project-2")
    print(f"  Project 2: {project2['path_with_namespace']} (id={project2['id']})")

    register_runner(base_url, pat)

    print("GitLab bootstrap complete\n")
    return {
        "pat": pat,
        "project_id": project_id,
        "project_path": project_path,
        "project2_id": project2["id"],
        "project2_path": project2["path_with_namespace"],
        "mr_iid": mr_iid,
        "issue_iid": issue["iid"],
        "base_url": base_url,
    }


if __name__ == "__main__":
    result = bootstrap_gitlab("http://127.0.0.1:8929")
    print(json.dumps(result, indent=2))
