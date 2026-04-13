#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR/../.."

echo "=== Starting Docker services ==="
docker compose -f test/e2e_docker/docker-compose.yml up -d

echo ""
echo "=== Waiting for services (GitLab takes 3-5 min) ==="
docker compose -f test/e2e_docker/docker-compose.yml wait gitlab jenkins 2>/dev/null || {
    echo "Waiting manually..."
    until curl -sf http://127.0.0.1:8929/-/readiness?all=1 > /dev/null 2>&1; do
        echo -n "."
        sleep 10
    done
    echo " GitLab ready"
    until curl -sf http://127.0.0.1:8930/login > /dev/null 2>&1; do
        echo -n "."
        sleep 5
    done
    echo " Jenkins ready"
}

echo ""
echo "=== Running E2E tests ==="
python3 test/e2e_docker/test_e2e.py --bootstrap
EXIT_CODE=$?

if [ "${KEEP_RUNNING:-}" != "1" ]; then
    echo ""
    echo "=== Tearing down ==="
    docker compose -f test/e2e_docker/docker-compose.yml down -v
fi

exit $EXIT_CODE
