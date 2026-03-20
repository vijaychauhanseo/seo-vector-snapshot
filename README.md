<p align="center">
  <img src="assets/hero-vector.svg" alt="SEO Vector Snapshot cover" width="100%" />
</p>

# SEO Vector Snapshot

Portable AI-search and technical SEO retrieval in a single repo.

This repository packages the live retrieval layer behind my local SEO squad into a portable SQLite snapshot plus CLI. The goal is simple: move the working memory graph to another laptop without rebuilding the whole system from zero.

## Why This Exists

Most SEO research gets trapped in tabs, docs, and half-remembered notes.

This repo turns that problem into a portable retrieval layer:

- a local `SQLite` vector-style memory snapshot
- a query tool that works outside my home directory
- task-pack routing for specialist SEO workflows
- a clean handoff point for another machine or another operator

## Snapshot At A Glance

| Metric | Value |
| --- | ---: |
| Captured | `2026-03-20` |
| Chunks | `2349` |
| Learned path priors | `345` |
| Learned skill priors | `106` |
| Learned pack priors | `21` |
| Role bundles | `190` |

<p align="center">
  <img src="assets/query-flow.svg" alt="How the SEO vector snapshot works" width="100%" />
</p>

## What Is Inside

- `db/squad_memory.db`
  - the portable retrieval snapshot
- `tools/squad_memory.py`
  - the query and routing CLI
- `tools/task_packs.json`
  - task-pack metadata used by the CLI
- `scripts/install_snapshot.sh`
  - one-command installer for another laptop
- `snapshot.json`
  - export metadata for this snapshot

## What You Can Use It For

- recover prior research on AI search, grounding, citations, and AI Overviews
- route a prompt toward the right SEO skill or memory pack
- move a working retrieval graph to a second laptop
- pair the DB with the companion [`seo-skills-pack`](https://github.com/vijaychauhanseo/seo-skills-pack)

## Quick Start

Clone the repo and query it in place:

```bash
git clone https://github.com/vijaychauhanseo/seo-vector-snapshot.git
cd seo-vector-snapshot
python3 tools/squad_memory.py query "selection rate grounding snippets" --db db/squad_memory.db
```

Install it to a local folder:

```bash
./scripts/install_snapshot.sh
```

Default install target:

- `~/squad_memory-portable`

Override the target:

```bash
./scripts/install_snapshot.sh /path/to/portable-memory
```

## Pair It With The Skills Pack

If the companion skills repo is not installed into `~/.codex/skills`, point the CLI at the cloned skills directory:

```bash
SQUAD_MEMORY_SKILLS_ROOT=../seo-skills-pack/skills \
python3 tools/squad_memory.py decide \
  "Need DEJAN-style AI search reverse engineering" \
  --db db/squad_memory.db
```

## Example Queries

```bash
python3 tools/squad_memory.py query "Gemini grounding classifier" --db db/squad_memory.db
python3 tools/squad_memory.py query "AI Mode page indexing and content store" --db db/squad_memory.db
python3 tools/squad_memory.py decide "Need a practitioner for core update plus AI Overviews" --db db/squad_memory.db
```

## Who This Is For

- technical SEOs building local research systems
- AI-search analysts who want portable memory, not fresh tab chaos
- operators moving a working Codex or OpenClaw setup between laptops
- founders building internal retrieval before full productization

## Portability Notes

- the bundled `tools/squad_memory.py` has been patched to avoid a hardcoded `/Users/vijaychauhan` dependency
- the DB snapshot is portable and can be queried in place
- no secrets are stored in this repository

## Companion Repository

The instruction and memory layer that pairs with this DB lives here:

- [`seo-skills-pack`](https://github.com/vijaychauhanseo/seo-skills-pack)

Use this repo for retrieval.
Use the skills repo for interpretation, routing, and operator context.

## Social Preview Asset

If you want a custom GitHub social preview card for this repo, use:

- `assets/social-preview.png`

## License

MIT. See [`LICENSE`](LICENSE).
