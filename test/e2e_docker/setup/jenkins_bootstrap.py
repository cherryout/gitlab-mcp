"""Bootstrap Jenkins for E2E tests.

Creates: folder, pipeline jobs, API token.
"""

import json
import time
import base64
import urllib.request
import urllib.error


PIPELINE_CONFIG = """<?xml version='1.0' encoding='UTF-8'?>
<flow-definition plugin="workflow-job">
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition" plugin="workflow-cps">
    <script>
pipeline {{
    agent any
    stages {{
        stage('Build') {{
            steps {{
                echo 'Building {name}...'
                sleep 5
            }}
        }}
        stage('Test') {{
            steps {{
                echo 'Testing {name}...'
                sleep 3
            }}
        }}
    }}
}}
    </script>
    <sandbox>true</sandbox>
  </definition>
</flow-definition>
"""

FAIL_PIPELINE_CONFIG = """<?xml version='1.0' encoding='UTF-8'?>
<flow-definition plugin="workflow-job">
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition" plugin="workflow-cps">
    <script>
pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                echo 'This will fail'
                error 'Intentional failure'
            }
        }
    }
}
    </script>
    <sandbox>true</sandbox>
  </definition>
</flow-definition>
"""

SLOW_PIPELINE_CONFIG = """<?xml version='1.0' encoding='UTF-8'?>
<flow-definition plugin="workflow-job">
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition" plugin="workflow-cps">
    <script>
pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                echo 'Long running...'
                sleep 120
            }
        }
    }
}
    </script>
    <sandbox>true</sandbox>
  </definition>
</flow-definition>
"""

UNSTABLE_PIPELINE_CONFIG = """<?xml version='1.0' encoding='UTF-8'?>
<flow-definition plugin="workflow-job">
  <definition class="org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition" plugin="workflow-cps">
    <script>
pipeline {
    agent any
    stages {
        stage('Test') {
            steps {
                echo 'Setting unstable'
                script {
                    currentBuild.result = 'UNSTABLE'
                }
            }
        }
    }
}
    </script>
    <sandbox>true</sandbox>
  </definition>
</flow-definition>
"""

FOLDER_CONFIG = """<?xml version='1.0' encoding='UTF-8'?>
<com.cloudbees.hudson.plugins.folder.Folder plugin="cloudbees-folder">
  <description>E2E test folder</description>
</com.cloudbees.hudson.plugins.folder.Folder>
"""


def jenkins_request(base_url: str, method: str, path: str,
                    auth: tuple[str, str], data: bytes | None = None,
                    content_type: str = "application/json") -> bytes:
    url = f"{base_url}{path}"
    headers = {
        "Content-Type": content_type,
        "Authorization": "Basic " + base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode(),
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        if e.code in (302, 409):
            return b""
        raise RuntimeError(f"{method} {path} -> {e.code}: {body[:200]}") from e


def wait_for_jenkins(base_url: str, timeout: int = 180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{base_url}/login", method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status < 500:
                    time.sleep(5)
                    return
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError(f"Jenkins not ready after {timeout}s")


def bootstrap_jenkins(base_url: str) -> dict:
    print("Bootstrapping Jenkins...")
    auth = ("admin", "admin")

    wait_for_jenkins(base_url)

    # Wait for plugin installation
    time.sleep(10)
    print("  Jenkins ready")

    # Create folder "SDK"
    jenkins_request(base_url, "POST", "/createItem?name=SDK", auth,
                    data=FOLDER_CONFIG.encode(), content_type="application/xml")
    print("  Folder SDK created")

    # Create folder "Frontend"
    jenkins_request(base_url, "POST", "/createItem?name=Frontend", auth,
                    data=FOLDER_CONFIG.encode(), content_type="application/xml")
    print("  Folder Frontend created")

    # Create pipeline "SDK/deploy_sdk_int"
    config = PIPELINE_CONFIG.format(name="sdk")
    jenkins_request(base_url, "POST", "/job/SDK/createItem?name=deploy_sdk_int", auth,
                    data=config.encode(), content_type="application/xml")
    print("  Job SDK/deploy_sdk_int created")

    # Create pipeline "Frontend/frontend_deploy_int"
    config = PIPELINE_CONFIG.format(name="frontend")
    jenkins_request(base_url, "POST", "/job/Frontend/createItem?name=frontend_deploy_int", auth,
                    data=config.encode(), content_type="application/xml")
    print("  Job Frontend/frontend_deploy_int created")

    # Create a job that always fails
    jenkins_request(base_url, "POST", "/job/SDK/createItem?name=deploy_sdk_fail", auth,
                    data=FAIL_PIPELINE_CONFIG.encode(), content_type="application/xml")
    print("  Job SDK/deploy_sdk_fail created")

    # Create a slow job (120s sleep) for testing build_started without completed
    jenkins_request(base_url, "POST", "/job/SDK/createItem?name=deploy_sdk_slow", auth,
                    data=SLOW_PIPELINE_CONFIG.encode(), content_type="application/xml")
    print("  Job SDK/deploy_sdk_slow created")

    # Create an unstable job (JUnit failures)
    jenkins_request(base_url, "POST", "/job/SDK/createItem?name=deploy_sdk_unstable", auth,
                    data=UNSTABLE_PIPELINE_CONFIG.encode(), content_type="application/xml")
    print("  Job SDK/deploy_sdk_unstable created")

    # Trigger initial build on SDK/deploy_sdk_int
    jenkins_request(base_url, "POST", "/job/SDK/job/deploy_sdk_int/build", auth)
    print("  Initial build triggered on SDK/deploy_sdk_int")

    # Wait for build to finish
    print("  Waiting for initial build...")
    time.sleep(15)

    # Generate API token
    resp = jenkins_request(base_url, "POST",
                           "/user/admin/descriptorByName/jenkins.security.ApiTokenProperty/generateNewToken",
                           auth, data=b"newTokenName=e2e-test",
                           content_type="application/x-www-form-urlencoded")
    token_data = json.loads(resp)
    api_token = token_data["data"]["tokenValue"]
    print(f"  API token generated")

    print("Jenkins bootstrap complete\n")
    return {
        "username": "admin",
        "password": "admin",
        "token": api_token,
        "jobs": {
            "sdk": "SDK/deploy_sdk_int",
            "frontend": "Frontend/frontend_deploy_int",
            "fail": "SDK/deploy_sdk_fail",
            "slow": "SDK/deploy_sdk_slow",
            "unstable": "SDK/deploy_sdk_unstable",
        },
        "base_url": base_url,
    }


if __name__ == "__main__":
    result = bootstrap_jenkins("http://127.0.0.1:8930")
    print(json.dumps(result, indent=2))
