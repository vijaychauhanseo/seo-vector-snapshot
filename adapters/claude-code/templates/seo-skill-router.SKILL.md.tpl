---
name: seo-skill-router
description: Route a task through the local SEO vector snapshot to find the best-fitting skill, memory pack, or operator lens.
argument-hint: [task]
disable-model-invocation: true
allowed-tools: Bash, Read
---

Route this SEO task: `$ARGUMENTS`

Run this exact command:

```bash
SQUAD_MEMORY_SKILLS_ROOT="__SKILLS_ROOT__" \
python3 "__CLI_PATH__" decide "$ARGUMENTS" --db "__DB_PATH__"
```

Rules:

1. Use the router output to identify the best skill, pack, or note set.
2. Explain why that route is appropriate in one short paragraph.
3. If the route points to DEJAN, grounding, AI Mode, or citation work, say that explicitly.
4. If the route looks weak, rerun once with a more concrete task phrasing.
