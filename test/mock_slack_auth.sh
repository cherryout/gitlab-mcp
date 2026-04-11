#!/bin/bash
# Mock slack-mcp-auth that launches mock_slack_mcp.py instead of the real server.
# The slack plugin calls: slack-mcp-auth mcp <workspace>
# We ignore args and just run the mock MCP.
exec python3 "$(dirname "$0")/mock_slack_mcp.py"
