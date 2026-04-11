#!/usr/bin/env python3
"""Mock Slack MCP server for testing slack-plugin.

Runs as a stdio JSON-RPC process, mimicking the real slack-mcp-server.
Responds to: initialize, tools/list, tools/call (channels_list, conversations_history).

State is loaded from a JSON file passed via MOCK_SLACK_STATE env var.
Tests write state to that file before spawning this process.
"""

import json
import os
import sys


def read_state() -> dict:
    path = os.environ.get("MOCK_SLACK_STATE", "")
    if path and os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"channels": [], "messages": {}}


def handle_initialize(req_id: int) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mock-slack-mcp", "version": "1.0"},
        },
    }


def handle_tools_list(req_id: int) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "result": {
            "tools": [
                {
                    "name": "channels_list",
                    "description": "List channels",
                    "inputSchema": {"type": "object", "properties": {}},
                },
                {
                    "name": "conversations_history",
                    "description": "Get channel history",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "channel_id": {"type": "string"},
                            "limit": {"type": "string"},
                        },
                    },
                },
                {
                    "name": "conversations_add_message",
                    "description": "Send message",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "channel_id": {"type": "string"},
                            "text": {"type": "string"},
                        },
                    },
                },
            ]
        },
    }


def handle_tool_call(req_id: int, name: str, args: dict) -> dict:
    state = read_state()

    if name == "channels_list":
        lines = []
        for ch in state.get("channels", []):
            ch_type = "im" if ch["id"].startswith("D") else "channel"
            lines.append(f'{ch["id"]} #{ch["name"]} ({ch_type})')
        text = "\n".join(lines)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"content": [{"type": "text", "text": text}]},
        }

    if name == "conversations_history":
        channel_id = args.get("channel_id", "")
        limit = int(args.get("limit", "10"))
        msgs = state.get("messages", {}).get(channel_id, [])
        msgs = msgs[:limit]

        lines = []
        for m in msgs:
            header = f'{m["user"]} ({m["ts"]})'
            if m.get("thread_ts"):
                header += f' thread:{m["thread_ts"]}'
            lines.append(header)
            lines.append(m.get("text", ""))
            lines.append("")

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"content": [{"type": "text", "text": "\n".join(lines)}]},
        }

    if name == "conversations_add_message":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"content": [{"type": "text", "text": "MsgID: mock_msg_1"}]},
        }

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Unknown tool: {name}"},
    }


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue

        if "id" not in msg:
            continue

        req_id = msg["id"]
        method = msg.get("method", "")
        params = msg.get("params", {})

        if method == "initialize":
            resp = handle_initialize(req_id)
        elif method == "tools/list":
            resp = handle_tools_list(req_id)
        elif method == "tools/call":
            tool_name = params.get("name", "")
            tool_args = params.get("arguments", {})
            resp = handle_tool_call(req_id, tool_name, tool_args)
        else:
            resp = {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Unknown method: {method}"},
            }

        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
