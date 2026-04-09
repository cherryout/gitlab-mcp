# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GitLab MCP (Model Context Protocol) Server - a TypeScript server that exposes GitLab API functionality through the MCP protocol. Supports 95+ GitLab operations including merge requests, issues, pipelines, wikis, milestones, and releases.

## Build & Development Commands

```bash
npm run build          # Compile TypeScript to build/
npm run dev            # Build and run server
npm run watch          # TypeScript watch mode
npm run lint           # ESLint check
npm run lint:fix       # ESLint auto-fix
npm run format         # Prettier format
npm run format:check   # Prettier check
```

## Testing Commands

```bash
npm test                    # API validation + remote auth tests
npm run test:integration    # API validation only (test/validate-api.js)
npm run test:remote-auth    # Remote authorization tests
npm run test:mcp:readonly   # Read-only MCP tests
npm run test:oauth          # OAuth tests
npm run test:all            # All tests combined
```

Tests require environment variables: `GITLAB_TOKEN_TEST` or `GITLAB_TOKEN`, and `TEST_PROJECT_ID`.

## Architecture

### Single-File Core Design

The server is implemented primarily in two large files:
- **index.ts** (~5000+ lines) - Main server logic containing:
  - MCP server initialization with `@modelcontextprotocol/sdk`
  - Transport modes: STDIO (default), SSE, Streamable HTTP
  - All GitLab API wrapper functions (e.g., `forkProject()`, `createIssue()`)
  - Tool definitions array with names, descriptions, and Zod schemas
  - Request handlers for `ListToolsRequestSchema` and `CallToolRequestSchema`
  - Session management for remote authorization
  - Cookie-based and OAuth authentication support

- **schemas.ts** (~2500+ lines) - All Zod schemas for:
  - GitLab API response types (e.g., `GitLabMergeRequestSchema`)
  - Tool input validation schemas (e.g., `CreateIssueSchema`)

- **oauth.ts** - OAuth2 PKCE flow implementation

### Tool Pattern

Tools follow a consistent pattern:
1. Define Zod schema in `schemas.ts`
2. Add tool definition to `allTools` array in `index.ts`
3. Implement API function in `index.ts`
4. Add case in `CallToolRequestSchema` handler switch statement

### Transport Modes

Controlled by environment variables:
- Default: STDIO transport
- `SSE=true`: Server-Sent Events on port 3002
- `STREAMABLE_HTTP=true`: HTTP streaming (preferred over SSE)
- `REMOTE_AUTHORIZATION=true`: Per-session token auth (requires Streamable HTTP)

### Feature Flags

Optional feature sets controlled by env vars:
- `USE_GITLAB_WIKI=true` - Wiki operations
- `USE_MILESTONE=true` - Milestone operations
- `USE_PIPELINE=true` - Pipeline/job operations
- `GITLAB_READ_ONLY_MODE=true` - Expose only read operations

## Key Environment Variables

- `GITLAB_PERSONAL_ACCESS_TOKEN` - GitLab PAT for authentication
- `GITLAB_API_URL` - GitLab API base URL (default: https://gitlab.com/api/v4)
- `GITLAB_PROJECT_ID` - Default project ID
- `GITLAB_ALLOWED_PROJECT_IDS` - Comma-separated list of allowed project IDs
- `GITLAB_USE_OAUTH=true` + `GITLAB_OAUTH_CLIENT_ID` - OAuth2 authentication

## Testing Notes

- Remote auth tests use a mock GitLab server (`test/utils/mock-gitlab-server.ts`)
- No real GitLab credentials required for remote auth tests
- Test clients for different transports in `test/clients/`
