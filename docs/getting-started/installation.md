# Installation

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager

## Install from Source

```bash
git clone <repo-url>
cd sqlprism
uv sync
```

This installs the package and all dependencies into a local `.venv`. All commands use `uv run sqlprism` to execute within this venv.

!!! tip
    If you activate the venv (`source .venv/bin/activate`), you can drop the `uv run` prefix and just use `sqlprism` directly.

## Verify Installation

```bash
uv run sqlprism --help
```

## First Run

```bash
# 1. Create default config
uv run sqlprism init

# 2. Edit ~/.sqlprism/config.json to add your repos
#    (see Configuration for details)

# 3. Index your SQL files
uv run sqlprism reindex

# 4. Check what was indexed
uv run sqlprism status
```

## MCP Client Setup

### Claude Code

```bash
claude mcp add sqlprism -- uv run --directory /path/to/sqlprism sqlprism serve
```

### Claude Desktop / Cursor / Continue.dev

Add to your `.mcp.json`:

```json
{
  "mcpServers": {
    "sqlprism": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/sqlprism", "sqlprism", "serve"]
    }
  }
}
```

Replace `/path/to/sqlprism` with the absolute path to your clone.
