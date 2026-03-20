#!/bin/zsh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLAUDE_HOME="${CLAUDE_HOME:-$HOME/.claude}"

mkdir -p "$CLAUDE_HOME/skills" "$CLAUDE_HOME"
CLAUDE_HOME="$(cd "$CLAUDE_HOME" && pwd)"
REPO_ROOT="$(cd "$REPO_ROOT" && pwd)"

MEMORY_PATH="$CLAUDE_HOME/seo-vector-snapshot.md"
SKILLS_ROOT="$CLAUDE_HOME/skills"
DB_PATH="$REPO_ROOT/db/squad_memory.db"
CLI_PATH="$REPO_ROOT/tools/squad_memory.py"
TASK_PACKS_PATH="$REPO_ROOT/tools/task_packs.json"

python3 "$REPO_ROOT/scripts/render_claude_adapter.py" \
  --template "$REPO_ROOT/adapters/claude-code/templates/seo-vector-snapshot.md.tpl" \
  --output "$MEMORY_PATH" \
  --repo-root "$REPO_ROOT" \
  --db-path "$DB_PATH" \
  --cli-path "$CLI_PATH" \
  --skills-root "$SKILLS_ROOT" \
  --task-packs-path "$TASK_PACKS_PATH"

mkdir -p "$CLAUDE_HOME/skills/seo-vector-query" "$CLAUDE_HOME/skills/seo-skill-router"

python3 "$REPO_ROOT/scripts/render_claude_adapter.py" \
  --template "$REPO_ROOT/adapters/claude-code/templates/seo-vector-query.SKILL.md.tpl" \
  --output "$CLAUDE_HOME/skills/seo-vector-query/SKILL.md" \
  --repo-root "$REPO_ROOT" \
  --db-path "$DB_PATH" \
  --cli-path "$CLI_PATH" \
  --skills-root "$SKILLS_ROOT" \
  --task-packs-path "$TASK_PACKS_PATH"

python3 "$REPO_ROOT/scripts/render_claude_adapter.py" \
  --template "$REPO_ROOT/adapters/claude-code/templates/seo-skill-router.SKILL.md.tpl" \
  --output "$CLAUDE_HOME/skills/seo-skill-router/SKILL.md" \
  --repo-root "$REPO_ROOT" \
  --db-path "$DB_PATH" \
  --cli-path "$CLI_PATH" \
  --skills-root "$SKILLS_ROOT" \
  --task-packs-path "$TASK_PACKS_PATH"

touch "$CLAUDE_HOME/CLAUDE.md"
IMPORT_LINE="@$MEMORY_PATH"
if ! rg -F "$IMPORT_LINE" "$CLAUDE_HOME/CLAUDE.md" >/dev/null 2>&1; then
  {
    printf "\n# SEO Vector Snapshot\n"
    printf "%s\n" "$IMPORT_LINE"
  } >> "$CLAUDE_HOME/CLAUDE.md"
fi

echo "Installed Claude adapter to: $CLAUDE_HOME"
echo "User memory import: $MEMORY_PATH"
echo "Installed skills:"
echo "- $CLAUDE_HOME/skills/seo-vector-query/SKILL.md"
echo "- $CLAUDE_HOME/skills/seo-skill-router/SKILL.md"
