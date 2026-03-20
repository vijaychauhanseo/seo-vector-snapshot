#!/bin/zsh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TARGET_DIR="${1:-$HOME/squad_memory-portable}"

mkdir -p "$TARGET_DIR"
cp "$REPO_ROOT/db/squad_memory.db" "$TARGET_DIR/squad_memory.db"
cp "$REPO_ROOT/tools/squad_memory.py" "$TARGET_DIR/squad_memory.py"
cp "$REPO_ROOT/tools/task_packs.json" "$TARGET_DIR/task_packs.json"

echo "Installed vector snapshot to $TARGET_DIR"
echo "Run: python3 $TARGET_DIR/squad_memory.py query \"grounding snippets\" --db $TARGET_DIR/squad_memory.db"
