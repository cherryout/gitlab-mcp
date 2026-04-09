#!/usr/bin/env python3
"""MCP client with channel support for testing MCP servers and channel plugins."""

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any, Callable

CONFIG_FILE = Path(__file__).parent / "mcp.json"


class MCPClient:
    """MCP client that connects to servers via stdio with channel notification support."""

    def __init__(self, command: str, args: list[str] = None, env: dict[str, str] = None):
        self.command = command
        self.args = args or []
        self.env = {**os.environ, **(env or {})}
        self.process: asyncio.subprocess.Process | None = None
        self._id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._reader_task: asyncio.Task | None = None
        self._on_notification: Callable[[dict], None] | None = None
        self._channel_enabled = False

    async def connect(self):
        cmd = [self.command] + self.args
        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self.env,
        )
        self._reader_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self):
        while True:
            line = await self.process.stdout.readline()
            if not line:
                break
            try:
                msg = json.loads(line.decode())
            except json.JSONDecodeError:
                continue

            if "id" in msg and msg["id"] in self._pending:
                self._pending[msg["id"]].set_result(msg)
            elif "method" in msg and "id" not in msg:
                if self._on_notification:
                    self._on_notification(msg)

    async def close(self):
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
        if self.process:
            self.process.terminate()
            await self.process.wait()

    async def _send(self, msg: dict):
        body = json.dumps(msg) + "\n"
        self.process.stdin.write(body.encode())
        await self.process.stdin.drain()

    async def request(self, method: str, params: dict = None, timeout: float = 30) -> dict:
        self._id += 1
        req_id = self._id
        msg = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params:
            msg["params"] = params

        future = asyncio.get_event_loop().create_future()
        self._pending[req_id] = future
        try:
            await self._send(msg)
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(req_id, None)

    async def notify(self, method: str, params: dict = None):
        msg = {"jsonrpc": "2.0", "method": method}
        if params:
            msg["params"] = params
        await self._send(msg)

    async def initialize(self, channel: bool = False) -> dict:
        capabilities = {}
        if channel:
            capabilities["experimental"] = {"claude/channel": {}}
            self._channel_enabled = True
        resp = await self.request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": capabilities,
            "clientInfo": {"name": "test-mcp-client", "version": "1.0"}
        })
        await self.notify("notifications/initialized")
        return resp

    async def list_tools(self) -> list[dict]:
        resp = await self.request("tools/list")
        return resp.get("result", {}).get("tools", [])

    async def list_resources(self) -> list[dict]:
        resp = await self.request("resources/list")
        return resp.get("result", {}).get("resources", [])

    async def read_resource(self, uri: str) -> Any:
        resp = await self.request("resources/read", {"uri": uri})
        if "error" in resp:
            return {"error": resp["error"]}
        result = resp.get("result", {})
        contents = result.get("contents", [])
        if contents:
            return contents[0].get("text", "")
        return result

    async def call_tool(self, name: str, arguments: dict) -> Any:
        resp = await self.request("tools/call", {"name": name, "arguments": arguments})
        if "error" in resp:
            return {"error": resp["error"]}
        result = resp.get("result", {})
        content = result.get("content", [])
        texts = []
        for item in content:
            if item.get("type") == "text":
                texts.append(item["text"])
        if texts:
            return "\n".join(texts)
        return result


def load_config() -> dict:
    if not CONFIG_FILE.exists():
        print(f"Config file not found: {CONFIG_FILE}", file=sys.stderr)
        sys.exit(1)
    with open(CONFIG_FILE) as f:
        return json.load(f).get("mcpServers", {})


def format_channel_event(msg: dict):
    method = msg.get("method", "")
    params = msg.get("params", {})

    if method == "notifications/claude/channel":
        content = params.get("content", "")
        meta = params.get("meta", {})
        event_type = meta.get("event_type", "unknown")
        source = meta.get("project", meta.get("source", ""))

        print(f"\n{'='*60}")
        print(f"  CHANNEL EVENT: {event_type}")
        for k, v in meta.items():
            print(f"  {k}: {v}")
        print(f"{'─'*60}")
        print(f"  {content}")
        print(f"{'='*60}\n")
    else:
        print(f"\n[notification] {method}: {json.dumps(params, indent=2)}\n")


def print_usage(servers: dict):
    print("Usage: mcp_client.py <server> [command] [args...]")
    print("\nAvailable servers:")
    for name in servers:
        print(f"  - {name}")
    print("\nCommands:")
    print("  (none)                   List available tools and resources")
    print("  <tool> [json]            Call a tool with optional JSON arguments")
    print("  resource <uri>           Read a resource by URI")
    print("  channel                  Listen for channel notifications (interactive)")
    print("  channel-send <content>   Simulate sending a channel notification to server")
    print("\nExamples:")
    print("  mcp_client.py slack")
    print('  mcp_client.py slack channels_list \'{"channel_types": "public_channel"}\'')
    print("  mcp_client.py slack resource slack://workspace/users")
    print("  mcp_client.py gitlab-channel channel")


async def channel_interactive(client: MCPClient):
    """Interactive channel mode: listen for notifications and allow tool calls."""
    print("Channel mode active. Listening for notifications...", file=sys.stderr)
    print("Type tool commands as: <tool_name> <json_args>", file=sys.stderr)
    print("Type 'tools' to list available tools", file=sys.stderr)
    print("Type 'quit' to exit\n", file=sys.stderr)

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()

    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, stop.set)

    async def read_stdin():
        reader = asyncio.StreamReader()
        await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
        return reader

    stdin_reader = await read_stdin()

    while not stop.is_set():
        try:
            line_bytes = await asyncio.wait_for(stdin_reader.readline(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        except EOFError:
            break

        if not line_bytes:
            break

        line = line_bytes.decode().strip()
        if not line:
            continue

        if line == "quit":
            break

        if line == "tools":
            tools = await client.list_tools()
            print("\nAvailable tools:")
            for t in tools:
                desc = t.get("description", "")[:80]
                print(f"  {t['name']}: {desc}")
            print()
            continue

        parts = line.split(None, 1)
        tool_name = parts[0]
        tool_args = json.loads(parts[1]) if len(parts) > 1 else {}
        try:
            result = await client.call_tool(tool_name, tool_args)
            if isinstance(result, str):
                print(f"\n[result] {result}\n")
            else:
                print(f"\n[result] {json.dumps(result, indent=2, default=str)}\n")
        except Exception as e:
            print(f"\n[error] {e}\n", file=sys.stderr)


async def main():
    servers = load_config()

    if len(sys.argv) < 2:
        print_usage(servers)
        sys.exit(1)

    server_name = sys.argv[1]
    command = sys.argv[2] if len(sys.argv) > 2 else None
    extra = sys.argv[3] if len(sys.argv) > 3 else None

    if server_name not in servers:
        print(f"Unknown server: {server_name}", file=sys.stderr)
        print_usage(servers)
        sys.exit(1)

    cfg = servers[server_name]
    client = MCPClient(cfg["command"], cfg.get("args", []), cfg.get("env", {}))

    try:
        print(f"Connecting to {server_name}...", file=sys.stderr)
        await client.connect()

        is_channel = command in ("channel", "channel-send")
        init_resp = await client.initialize(channel=is_channel)

        server_caps = init_resp.get("result", {}).get("capabilities", {})
        has_channel = "claude/channel" in server_caps.get("experimental", {})

        if is_channel and has_channel:
            print(f"Server declares channel capability.", file=sys.stderr)
        elif is_channel:
            print(f"Warning: server does not declare channel capability.", file=sys.stderr)

        print("Connected.\n", file=sys.stderr)

        if command is None:
            tools = await client.list_tools()
            print("Available tools:")
            for t in tools:
                desc = t.get("description", "")[:80]
                print(f"  {t['name']}: {desc}")

            resources = await client.list_resources()
            if resources:
                print("\nAvailable resources:")
                for r in resources:
                    print(f"  {r.get('uri', r.get('name', ''))}")

        elif command == "channel":
            client._on_notification = format_channel_event
            await channel_interactive(client)

        elif command == "channel-send":
            content = extra or "test notification"
            await client.notify("notifications/claude/channel", {
                "content": content,
                "meta": {"event_type": "test", "source": "mcp_client"},
            })
            print(f"Sent channel notification: {content}")

        elif command == "resource":
            uri = extra or ""
            if not uri:
                print("Error: resource URI required", file=sys.stderr)
                sys.exit(1)
            print(f"Reading resource {uri}...", file=sys.stderr)
            result = await client.read_resource(uri)
            print(result)

        else:
            args = json.loads(extra) if extra else {}
            print(f"Calling {command}({json.dumps(args)})...", file=sys.stderr)
            result = await client.call_tool(command, args)
            if isinstance(result, str):
                print(result)
            else:
                print(json.dumps(result, indent=2, default=str))
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
