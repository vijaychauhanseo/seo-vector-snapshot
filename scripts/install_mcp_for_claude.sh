#!/bin/zsh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SCOPE="${1:-user}"
SERVER_NAME="${2:-seo-memory}"
SKILLS_ROOT="${SQUAD_MEMORY_SKILLS_ROOT:-$HOME/.claude/skills}"
DB_PATH="$REPO_ROOT/db/squad_memory.db"
TASK_PACKS_PATH="$REPO_ROOT/tools/task_packs.json"
SERVER_PATH="$REPO_ROOT/mcp/seo_memory_mcp_server.py"

if command -v claude >/dev/null 2>&1; then
  claude mcp add "$SERVER_NAME" --scope "$SCOPE" -- \
    python3 "$SERVER_PATH" \
      --db "$DB_PATH" \
      --skills-root "$SKILLS_ROOT" \
      --task-packs "$TASK_PACKS_PATH"
  echo "Added MCP server '$SERVER_NAME' to Claude ($SCOPE scope)."
  echo "Verify with: claude mcp get $SERVER_NAME"
  exit 0
fi

cat <<EOF
Claude CLI was not found.

Use this command on a machine with Claude Code installed:

claude mcp add $SERVER_NAME --scope $SCOPE -- \\
  python3 "$SERVER_PATH" \\
    --db "$DB_PATH" \\
    --skills-root "$SKILLS_ROOT" \\
    --task-packs "$TASK_PACKS_PATH"

Or use the JSON example at:
  $REPO_ROOT/mcp/claude.mcp.json.example
EOF
