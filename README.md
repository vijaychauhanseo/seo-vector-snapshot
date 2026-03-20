# SEO Vector Snapshot

Portable snapshot of the local `squad_memory` retriever and DB.

## Contents

- `db/squad_memory.db`
- `tools/squad_memory.py`
- `tools/task_packs.json`
- `snapshot.json`
- `scripts/install_snapshot.sh`

## Snapshot

- Captured: `2026-03-20`
- Chunks: `2349`
- Learned path priors: `345`
- Learned skill priors: `106`
- Learned pack priors: `21`
- Role bundles: `190`

## Use In Place

Query the snapshot directly from the repo:

```bash
python3 tools/squad_memory.py query "selection rate grounding snippets" --db db/squad_memory.db
```

If your SEO skills are not installed into `~/.codex/skills` on the target laptop, point the tool at the cloned skills repo:

```bash
SQUAD_MEMORY_SKILLS_ROOT=../seo-skills-pack/skills \
python3 tools/squad_memory.py decide "Need DEJAN-style AI search reverse engineering" --db db/squad_memory.db
```

## Install To A Local Folder

```bash
./scripts/install_snapshot.sh
```

Default install target:

- `~/squad_memory-portable`

Override the target:

```bash
./scripts/install_snapshot.sh /path/to/portable-memory
```

## Notes

- The portable `tools/squad_memory.py` copy no longer depends on `/Users/vijaychauhan`.
- Chunk paths inside the DB are relative, so the snapshot is portable.
- This repo does not contain secrets.
