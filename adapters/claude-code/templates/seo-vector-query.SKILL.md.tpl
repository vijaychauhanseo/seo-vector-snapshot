---
name: seo-vector-query
description: Query the local SEO vector snapshot database. Use when you need to search stored SEO memory, DEJAN notes, practitioner canon, or AI-search research.
argument-hint: [query]
disable-model-invocation: true
allowed-tools: Bash, Read
---

Query the local SEO vector snapshot for: `$ARGUMENTS`

Run this exact command:

```bash
python3 "__CLI_PATH__" query "$ARGUMENTS" --db "__DB_PATH__" --top 8
```

Rules:

1. Use the command output as the primary retrieval step.
2. Summarize the most relevant hits for the user.
3. Mention if the result suggests using a deeper SEO skill or DEJAN reverse-engineering skill.
4. If the output is weak, suggest a better query string and rerun once.
