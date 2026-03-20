#!/usr/bin/env python3
"""Portable MCP server for the SEO vector snapshot."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = REPO_ROOT / "tools"
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import squad_memory  # noqa: E402


SERVER_NAME = "seo-memory"
SERVER_TITLE = "SEO Memory MCP"
SERVER_VERSION = "0.1.0"
LATEST_PROTOCOL_VERSION = "2025-06-18"
SUPPORTED_PROTOCOL_VERSIONS = {
    "2024-11-05",
    "2025-03-26",
    "2025-06-18",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MCP server for the SEO vector snapshot")
    parser.add_argument("--db", default=str(REPO_ROOT / "db" / "squad_memory.db"))
    parser.add_argument("--skills-root", default=str(Path.home() / ".claude" / "skills"))
    parser.add_argument("--task-packs", default=str(REPO_ROOT / "tools" / "task_packs.json"))
    return parser.parse_args()


def read_message() -> Optional[Dict[str, Any]]:
    headers: Dict[str, str] = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in (b"\r\n", b"\n"):
            break
        decoded = line.decode("utf-8").strip()
        if not decoded:
            continue
        key, value = decoded.split(":", 1)
        headers[key.strip().lower()] = value.strip()

    length_value = headers.get("content-length")
    if not length_value:
        raise ValueError("Missing Content-Length header")
    length = int(length_value)
    payload = sys.stdin.buffer.read(length)
    if not payload:
        return None
    return json.loads(payload.decode("utf-8"))


def write_message(message: Dict[str, Any]) -> None:
    raw = json.dumps(message, ensure_ascii=True).encode("utf-8")
    sys.stdout.buffer.write(f"Content-Length: {len(raw)}\r\n\r\n".encode("ascii"))
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()


def clamp_top(value: Any, default: int = 5, max_value: int = 20) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(parsed, max_value))


def build_snapshot_info(db_path: Path, skills_root: Path, task_packs_path: Path) -> Dict[str, Any]:
    counts: Dict[str, int] = {}
    with sqlite3.connect(str(db_path)) as con:
        for table in ("chunks", "learned_path_priors", "learned_skill_priors", "learned_pack_priors", "role_bundles"):
            counts[table] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return {
        "server": SERVER_NAME,
        "db_path": str(db_path),
        "skills_root": str(skills_root),
        "task_packs_path": str(task_packs_path),
        "counts": counts,
    }


def summarize_query(payload: Dict[str, Any]) -> str:
    lines = [f"Query: {payload['query']}"]
    if payload.get("role"):
        lines.append(f"Role: {payload['role']}")
    results = payload.get("results", [])
    if not results:
        lines.append("No matching memory hits found.")
        return "\n".join(lines)
    lines.append("Top results:")
    for item in results[: min(len(results), 5)]:
        lines.append(
            f"- [{item['score']}] {item['path']} :: {item['heading']} "
            f"(skill={item['skill']}, source={item['source'] or '-'})"
        )
    return "\n".join(lines)


def summarize_decision(payload: Dict[str, Any]) -> str:
    lines = [f"Task: {payload['query']}"]
    if payload.get("role"):
        lines.append(f"Role: {payload['role']}")
    skills = payload.get("recommended_skills", [])
    if skills:
        lines.append("Recommended skills:")
        for item in skills[: min(len(skills), 4)]:
            lines.append(f"- {item['skill']} [{item['score']}]")
    memory = payload.get("supporting_memory", [])
    if memory:
        lines.append("Supporting memory:")
        for item in memory[: min(len(memory), 4)]:
            lines.append(f"- {item['path']} :: {item['heading']}")
    return "\n".join(lines)


def summarize_task_pack(payload: Dict[str, Any]) -> str:
    pack = payload["selected_pack"]
    lines = [
        f"Task: {payload['query']}",
        f"Selected pack: {pack['id']} ({pack['name']}) [{pack['score']}]",
        f"Primary skill: {pack['primary_skill']}",
    ]
    if pack.get("supporting_skills"):
        lines.append(f"Supporting skills: {', '.join(pack['supporting_skills'])}")
    reasons = pack.get("reasons", [])
    if reasons:
        lines.append("Why this pack:")
        lines.extend(f"- {reason}" for reason in reasons[:4])
    return "\n".join(lines)


def summarize_execution_plan(payload: Dict[str, Any]) -> str:
    pack = payload["selected_pack"]
    lines = [
        f"Task: {payload['query']}",
        f"Execution pack: {pack['id']} ({pack['name']})",
        "Execution steps:",
    ]
    lines.extend(f"{idx}. {step}" for idx, step in enumerate(payload.get("execution_steps", [])[:6], start=1))
    return "\n".join(lines)


def summarize_snapshot_info(payload: Dict[str, Any]) -> str:
    counts = payload["counts"]
    return "\n".join(
        [
            f"Server: {payload['server']}",
            f"DB: {payload['db_path']}",
            f"Skills root: {payload['skills_root']}",
            f"Task packs: {payload['task_packs_path']}",
            f"Chunks: {counts['chunks']}",
            f"Learned path priors: {counts['learned_path_priors']}",
            f"Learned skill priors: {counts['learned_skill_priors']}",
            f"Learned pack priors: {counts['learned_pack_priors']}",
            f"Role bundles: {counts['role_bundles']}",
        ]
    )


def tool_definitions() -> list[Dict[str, Any]]:
    return [
        {
            "name": "seo_memory_query",
            "title": "SEO Memory Query",
            "description": "Search the local SEO memory snapshot for practitioner notes, DEJAN research, AI-search references, and technical SEO memory chunks.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query to run against the local memory snapshot."},
                    "top": {"type": "integer", "description": "Maximum number of results to return.", "default": 8, "minimum": 1, "maximum": 20},
                    "role": {"type": "string", "description": "Optional role hint such as pinchy, coral, or kelp."},
                    "skill": {"type": "string", "description": "Optional skill filter such as seo or dejan-ai-reverse-engineering."},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
        },
        {
            "name": "seo_skill_route",
            "title": "SEO Skill Route",
            "description": "Route a task to the best-fitting SEO skill and supporting memory using the local snapshot.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task": {"type": "string", "description": "The SEO or AI-search task to route."},
                    "top": {"type": "integer", "description": "Maximum number of skills to rank.", "default": 5, "minimum": 1, "maximum": 20},
                    "role": {"type": "string", "description": "Optional role hint such as pinchy, coral, or kelp."},
                },
                "required": ["task"],
                "additionalProperties": False,
            },
        },
        {
            "name": "seo_task_pack",
            "title": "SEO Task Pack",
            "description": "Resolve the best reusable SEO task pack for a request, including the primary skill and memory themes.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task": {"type": "string", "description": "The task or request to map to a reusable task pack."},
                    "top": {"type": "integer", "description": "Maximum number of candidate packs to consider.", "default": 5, "minimum": 1, "maximum": 20},
                    "role": {"type": "string", "description": "Optional role hint such as pinchy, coral, or kelp."},
                    "pack_id": {"type": "string", "description": "Optional explicit pack override."},
                },
                "required": ["task"],
                "additionalProperties": False,
            },
        },
        {
            "name": "seo_execution_plan",
            "title": "SEO Execution Plan",
            "description": "Build an execution plan from the best task pack for an SEO or AI-search workflow.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task": {"type": "string", "description": "The task or request to convert into an execution plan."},
                    "top": {"type": "integer", "description": "Maximum number of candidate packs to consider.", "default": 5, "minimum": 1, "maximum": 20},
                    "role": {"type": "string", "description": "Optional role hint such as pinchy, coral, or kelp."},
                    "pack_id": {"type": "string", "description": "Optional explicit pack override."},
                },
                "required": ["task"],
                "additionalProperties": False,
            },
        },
        {
            "name": "seo_snapshot_info",
            "title": "SEO Snapshot Info",
            "description": "Inspect the local SEO snapshot and return paths plus key counts for chunks, priors, and role bundles.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
    ]


def make_text_result(text: str, structured: Dict[str, Any], is_error: bool = False) -> Dict[str, Any]:
    return {
        "content": [{"type": "text", "text": text}],
        "structuredContent": structured,
        "isError": is_error,
    }


class SeoMemoryMcpServer:
    def __init__(self, db_path: Path, skills_root: Path, task_packs_path: Path) -> None:
        self.db_path = db_path
        self.skills_root = skills_root
        self.task_packs_path = task_packs_path
        squad_memory.SKILLS_ROOT = skills_root

    def initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        client_version = params.get("protocolVersion", LATEST_PROTOCOL_VERSION)
        protocol_version = client_version if client_version in SUPPORTED_PROTOCOL_VERSIONS else LATEST_PROTOCOL_VERSION
        return {
            "protocolVersion": protocol_version,
            "capabilities": {
                "tools": {
                    "listChanged": False,
                }
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "title": SERVER_TITLE,
                "version": SERVER_VERSION,
                "description": "Portable MCP server for the SEO vector snapshot and skill router.",
            },
            "instructions": (
                "Use the SEO memory tools to query practitioner notes, route tasks to the right SEO skill, "
                "and build reusable execution plans from the local snapshot."
            ),
        }

    def list_tools(self) -> Dict[str, Any]:
        return {"tools": tool_definitions()}

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        args = arguments or {}
        try:
            if name == "seo_memory_query":
                payload = {
                    "query": args["query"],
                    "role": args.get("role"),
                    "skill_filter": args.get("skill"),
                    "results": squad_memory.rank_chunks(
                        self.db_path,
                        args["query"],
                        role=args.get("role"),
                        skill_filter=args.get("skill"),
                        top=clamp_top(args.get("top"), default=8),
                    ),
                }
                return make_text_result(summarize_query(payload), payload)

            if name == "seo_skill_route":
                payload = squad_memory.decide(
                    self.db_path,
                    args["task"],
                    role=args.get("role"),
                    top=clamp_top(args.get("top"), default=5),
                )
                return make_text_result(summarize_decision(payload), payload)

            if name == "seo_task_pack":
                payload = squad_memory.resolve_task_pack(
                    self.db_path,
                    self.task_packs_path,
                    args["task"],
                    args.get("role"),
                    clamp_top(args.get("top"), default=5),
                    pack_id=args.get("pack_id"),
                )
                return make_text_result(summarize_task_pack(payload), payload)

            if name == "seo_execution_plan":
                payload = squad_memory.build_execute_plan(
                    self.db_path,
                    self.task_packs_path,
                    args["task"],
                    args.get("role"),
                    clamp_top(args.get("top"), default=5),
                    pack_id=args.get("pack_id"),
                )
                return make_text_result(summarize_execution_plan(payload), payload)

            if name == "seo_snapshot_info":
                payload = build_snapshot_info(self.db_path, self.skills_root, self.task_packs_path)
                return make_text_result(summarize_snapshot_info(payload), payload)
        except KeyError as exc:
            return make_text_result(
                f"Missing required argument: {exc.args[0]}",
                {"tool": name, "missing_argument": exc.args[0]},
                is_error=True,
            )
        except Exception as exc:  # pragma: no cover - runtime safety
            return make_text_result(
                f"{type(exc).__name__}: {exc}",
                {"tool": name, "error": type(exc).__name__, "message": str(exc)},
                is_error=True,
            )

        raise ValueError(f"Unknown tool: {name}")


def success_response(message_id: Any, result: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def error_response(message_id: Any, code: int, message: str) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}}


def serve(server: SeoMemoryMcpServer) -> None:
    while True:
        request = read_message()
        if request is None:
            break

        method = request.get("method")
        message_id = request.get("id")
        params = request.get("params") or {}

        # Notifications do not receive responses.
        if message_id is None:
            if method == "notifications/initialized":
                continue
            continue

        try:
            if method == "initialize":
                write_message(success_response(message_id, server.initialize(params)))
            elif method == "ping":
                write_message(success_response(message_id, {}))
            elif method == "tools/list":
                write_message(success_response(message_id, server.list_tools()))
            elif method == "tools/call":
                tool_name = params.get("name")
                if not tool_name:
                    write_message(error_response(message_id, -32602, "Missing tool name"))
                    continue
                write_message(success_response(message_id, server.call_tool(tool_name, params.get("arguments"))))
            else:
                write_message(error_response(message_id, -32601, f"Method not found: {method}"))
        except Exception as exc:  # pragma: no cover - protocol safety
            write_message(error_response(message_id, -32603, f"Internal error: {type(exc).__name__}: {exc}"))


def main() -> None:
    args = parse_args()
    server = SeoMemoryMcpServer(
        db_path=Path(args.db).expanduser().resolve(),
        skills_root=Path(args.skills_root).expanduser().resolve(),
        task_packs_path=Path(args.task_packs).expanduser().resolve(),
    )
    serve(server)


if __name__ == "__main__":
    main()
