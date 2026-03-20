#!/usr/bin/env python3
import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--cli-path", required=True)
    parser.add_argument("--skills-root", required=True)
    parser.add_argument("--task-packs-path", required=True)
    args = parser.parse_args()

    template = Path(args.template).read_text()
    rendered = (
        template
        .replace("__REPO_ROOT__", args.repo_root)
        .replace("__DB_PATH__", args.db_path)
        .replace("__CLI_PATH__", args.cli_path)
        .replace("__SKILLS_ROOT__", args.skills_root)
        .replace("__TASK_PACKS_PATH__", args.task_packs_path)
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered)


if __name__ == "__main__":
    main()
