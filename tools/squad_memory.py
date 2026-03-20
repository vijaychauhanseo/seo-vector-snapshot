#!/usr/bin/env python3
"""Local hybrid retriever for Codex squad skills and memory.

This tool indexes installed skills, memory notes, and router documents into:
1. SQLite FTS5 for fast lexical recall
2. A pure-Python sparse TF-IDF layer for reranking

It is designed for environments without vector DBs or ML dependencies.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import os
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


SCRIPT_PATH = Path(__file__).resolve()
TOOLS_DIR = SCRIPT_PATH.parent
REPO_ROOT = TOOLS_DIR.parent
HOME = Path.home()


def _resolve_default_db_path() -> Path:
    local_db = TOOLS_DIR / "squad_memory.db"
    repo_db = REPO_ROOT / "db" / "squad_memory.db"
    if local_db.exists():
        return local_db
    if repo_db.exists():
        return repo_db
    return HOME / "squad_memory" / "squad_memory.db"


def _resolve_default_task_packs_path() -> Path:
    local_task_packs = TOOLS_DIR / "task_packs.json"
    if local_task_packs.exists():
        return local_task_packs
    return HOME / "squad_memory" / "task_packs.json"


def _resolve_default_skills_root() -> Path:
    env_value = os.environ.get("SQUAD_MEMORY_SKILLS_ROOT")
    if env_value:
        return Path(env_value).expanduser()
    return HOME / ".codex" / "skills"


SKILLS_ROOT = _resolve_default_skills_root()
DB_PATH = Path(os.environ.get("SQUAD_MEMORY_DB", str(_resolve_default_db_path()))).expanduser()
TASK_PACKS_PATH = Path(
    os.environ.get("SQUAD_MEMORY_TASK_PACKS", str(_resolve_default_task_packs_path()))
).expanduser()

TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_\-]{1,}")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")
ISO_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
MONTH_DATE_RE = re.compile(
    r"\b("
    r"January|February|March|April|May|June|July|August|September|October|November|December"
    r")\s+\d{1,2},\s+20\d{2}\b",
    re.IGNORECASE,
)
DEFAULT_VECTOR_DIM_LIMIT = 48
DEFAULT_TOKEN_CONTEXT_LIMIT = 24
MAX_SEMANTIC_DF_RATIO = 0.25
CONFIDENCE_BOOSTS = {
    "high": 0.05,
    "medium": 0.025,
    "low": 0.0,
}

QUERY_INTENT_RULES = [
    {
        "intent": "ai_visibility",
        "patterns": [r"\bai visibility\b", r"\bbrand mentions?\b", r"\bcitations?\b", r"\bimpressions?\b", r"\bbrand radar\b"],
        "terms": ["ai_visibility", "brand_mentions", "citations", "impressions", "brand_radar", "topic_association"],
    },
    {
        "intent": "ai_selection",
        "patterns": [
            r"selected into ai answers",
            r"not being selected",
            r"not getting cited",
            r"not being cited",
            r"answer inclusion",
            r"snippet extraction",
            r"grounding snippets?",
            r"selection rate",
        ],
        "terms": ["selection_rate", "grounding", "grounding_snippets", "snippet_extraction", "selection", "cited", "answer_inclusion"],
    },
    {
        "intent": "fan_out",
        "patterns": [r"fan[- ]?out", r"hidden queries?", r"implicit queries?", r"sub[- ]queries?"],
        "terms": ["fan_out", "hidden_queries", "implicit_queries", "query_expansion", "subqueries"],
    },
    {
        "intent": "leak_systems",
        "patterns": [r"\bnavboost\b", r"\bcraps\b", r"\bgoogle leak\b", r"\bsite quality\b", r"\btopicality\b"],
        "terms": ["navboost", "craps", "google_leak", "site_quality", "topicality", "quality_signals"],
    },
    {
        "intent": "content_writing",
        "patterns": [r"\bblog post\b", r"\bdraft\b", r"\bhook\b", r"\boutline\b", r"\bstructure\b", r"\bcopy\b"],
        "terms": ["writing", "editorial", "hook", "outline", "structure", "draft"],
    },
    {
        "intent": "code_review",
        "patterns": [
            r"\bcode review\b",
            r"\breviewer[- ]level\b",
            r"\breview focused\b",
            r"\bmaintainability risks?\b",
            r"\bsecurity regressions?\b",
            r"\bperformance regressions?\b",
        ],
        "terms": ["review", "code_review", "correctness", "security", "performance", "maintainability", "patch"],
    },
    {
        "intent": "support_triage",
        "patterns": [r"\btriage\b", r"\bcustomer issue\b", r"\bescalate\b", r"\bseverity\b", r"\bsupport\b"],
        "terms": ["support", "ticket", "triage", "severity", "escalation", "troubleshooting"],
    },
    {
        "intent": "implementation_review",
        "patterns": [r"\bfix\b.*\bbug\b", r"\bproduction bug\b", r"\bwrite tests\b", r"\breview\b.*\bpatch\b", r"\bbugfix\b"],
        "terms": ["developer", "implementation", "bugfix", "tests", "review", "patch", "feature"],
    },
    {
        "intent": "coordination",
        "patterns": [
            r"\bcoordinate\b",
            r"\bhandoffs?\b",
            r"\bcross[- ]functional\b",
            r"\bkeep the handoffs clean\b",
            r"\bwriter\b.*\bseo\b.*\bmarketing\b",
            r"\bspecialists?\b.*\bstep",
        ],
        "terms": [
            "coordination",
            "handoff",
            "orchestration",
            "workflow",
            "owners",
            "orchestrator",
            "pinchy",
            "chief_of_staff",
            "routing",
            "route",
        ],
    },
    {
        "intent": "fact_check",
        "patterns": [
            r"\bfact[- ]?check\b",
            r"\bverify claims?\b",
            r"\bsourced\b",
            r"\bsources?\b",
            r"\blandscape\b",
            r"\bcompetitor report\b",
        ],
        "terms": ["fact_check", "verification", "sources", "evidence", "research", "brief"],
    },
    {
        "intent": "social_creator",
        "patterns": [
            r"\bcreator-style\b",
            r"\bsocial channels?\b",
            r"\bengagement routine\b",
            r"\bposting plan\b",
            r"\bcontent calendar\b",
            r"\brepurpose\b.*\bsocial posts?\b",
            r"\blinkedin\b.*\bx\b",
            r"\bsocial posts?\b",
        ],
        "terms": ["social_media", "posting", "engagement", "creator", "content_calendar", "audience", "repurpose", "linkedin", "x"],
    },
]

INTENT_SKILL_PRIORS = {
    "ai_visibility": {"seo": 0.7, "seo-coral": 0.45, "ahrefs": 0.35, "marketing": 0.15},
    "ai_selection": {"dejan-ai-reverse-engineering": 1.95, "seo": 0.2},
    "fan_out": {"dejan-ai-reverse-engineering": 0.9, "seo": 0.25},
    "leak_systems": {"seo": 0.55},
    "content_writing": {"writer": 0.8, "writer-plankton": 0.6, "seo": 0.2},
    "code_review": {"reviewer": 1.35, "reviewer-barnacle": 1.05, "developer": 0.15, "qa": 0.1},
    "support_triage": {"support-anemone": 1.6, "support": 0.2, "operations": 0.15},
    "implementation_review": {"developer": 1.0, "developer-chitin": 0.7, "reviewer": 0.45, "qa": 0.35},
    "coordination": {"orchestrator-pinchy": 1.9, "operations": 0.7, "multi-agent-reef": 0.55},
    "fact_check": {"researcher": 1.25, "researcher-kelp": 0.95, "writer": 0.15},
    "social_creator": {"charles": 1.2, "marketing": 0.5, "marketing-current": 0.35},
}

ROLE_INTENT_SKILL_PRIORS = {
    ("pinchy", "coordination"): {
        "orchestrator-pinchy": 5.9,
        "operations": 1.25,
        "multi-agent-reef": 0.9,
    },
}

INTENT_VARIANT_PREFERENCES = {
    "support_triage": [("support", "support-anemone")],
}

OPENAI_QUERY_HINTS = {"openai", "chatgpt", "api", "gpt", "responses", "assistants", "model", "models"}
GLOBAL_BUCKET = "__all__"
SUGGESTION_STOPWORDS = {
    "need",
    "plan",
    "help",
    "and",
    "with",
    "from",
    "that",
    "this",
    "into",
    "your",
    "our",
    "their",
    "they",
    "them",
    "what",
    "when",
    "where",
    "which",
    "while",
    "for",
    "would",
    "should",
    "could",
    "about",
    "using",
    "used",
    "more",
    "best",
    "better",
    "than",
    "have",
    "make",
    "made",
    "like",
    "just",
    "page",
    "pages",
    "site",
    "sites",
    "domain",
    "mode",
    "query",
    "queries",
    "task",
    "tasks",
    "squad",
    "search",
    "reverse",
    "engineering",
    "style",
}

SKILL_ALIASES = {
    "orchestrator-pinchy": ["pinchy", "orchestrator", "chief_of_staff", "coordination"],
    "seo": ["coral", "seo", "search", "organic", "rankings", "visibility"],
    "seo-coral": ["coral", "seo", "search", "organic", "rankings", "visibility"],
    "ahrefs": ["ahrefs", "backlinks", "refdomains", "traffic_value", "keyword_gap"],
    "dejan-ai-reverse-engineering": [
        "dejan",
        "dan_petrovic",
        "reverse_engineering",
        "ai_mode",
        "grounding",
        "grounding_snippets",
        "snippet_extraction",
        "selection_rate",
        "selection",
        "selected",
        "cited",
        "citations",
        "answer_inclusion",
        "sro",
        "fan_out",
        "machine_readability",
        "primary_bias",
    ],
    "programmatic-seo": ["programmatic", "pseo", "template_pages", "scale_pages"],
    "writer": ["plankton", "writer", "copywriting", "blog", "landing_page", "email"],
    "writer-plankton": ["plankton", "writer", "copywriting", "blog", "landing_page", "email"],
    "charles": ["charles", "social", "social_media", "threads", "linkedin", "tiktok", "youtube"],
    "marketing": ["current", "marketing", "growth", "distribution", "promotion", "campaigns"],
    "marketing-current": ["current", "marketing", "growth", "distribution", "promotion", "campaigns"],
    "developer": ["chitin", "developer", "code", "implementation", "bugfix", "feature"],
    "developer-chitin": ["chitin", "developer", "code", "implementation", "bugfix", "feature"],
    "devops": ["tide", "devops", "deployment", "infra", "infrastructure", "pipeline"],
    "devops-tide": ["tide", "devops", "deployment", "infra", "infrastructure", "pipeline"],
    "qa": ["reef", "qa", "testing", "regression", "verification", "pass_fail"],
    "qa-reef": ["reef", "qa", "testing", "regression", "verification", "pass_fail"],
    "researcher": ["kelp", "research", "context", "fact_check", "sources"],
    "researcher-kelp": ["kelp", "research", "context", "fact_check", "sources"],
    "reviewer": ["barnacle", "review", "code_review", "quality_gate", "request_changes"],
    "reviewer-barnacle": ["barnacle", "review", "code_review", "quality_gate", "request_changes"],
    "support": ["anemone", "support", "tickets", "triage", "customer_issue"],
    "support-anemone": ["anemone", "support", "tickets", "triage", "customer_issue"],
    "operations": ["urchin", "operations", "project_management", "status", "timeline"],
    "operations-urchin": ["urchin", "operations", "project_management", "status", "timeline"],
    "finance": ["krill", "finance", "invoice", "cash_flow", "expenses"],
    "finance-krill": ["krill", "finance", "invoice", "cash_flow", "expenses"],
    "emily": ["emily", "design", "graphic_design", "visuals", "brand_design", "ui"],
    "multi-agent-reef": ["multi_agent", "orchestration", "handoff", "workflow", "specialists"],
}


@dataclasses.dataclass
class DocChunk:
    chunk_id: str
    path: str
    skill: str
    file_type: str
    heading: str
    text: str
    section_kind: str
    source: str
    published_on: str
    freshness: float
    topics: List[str] = dataclasses.field(default_factory=list)
    intents: List[str] = dataclasses.field(default_factory=list)
    use_for: List[str] = dataclasses.field(default_factory=list)
    avoid_for: List[str] = dataclasses.field(default_factory=list)
    confidence: str = ""
    tags: List[str] = dataclasses.field(default_factory=list)
    roles: List[str] = dataclasses.field(default_factory=list)
    is_canonical: bool = False
    canonical_group: str = ""
    bundles: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class SquadRouter:
    skill_tags: Dict[str, List[str]]
    role_paths: Dict[str, List[str]]
    path_bundles: Dict[str, List[str]]


@dataclasses.dataclass
class TaskPack:
    pack_id: str
    name: str
    description: str
    roles: List[str]
    intents: List[str]
    keywords: List[str]
    primary_skill: str
    supporting_skills: List[str]
    memory_focus: List[str]
    checklist: List[str]
    deliverables: List[str]
    output_sections: List[str]
    handoffs: List[str]
    escalation_rules: List[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hybrid memory retriever for Codex skills")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="Build or rebuild the local index")
    build.add_argument("--root", default=str(SKILLS_ROOT))
    build.add_argument("--db", default=str(DB_PATH))

    query = subparsers.add_parser("query", help="Query memory chunks")
    query.add_argument("text", help="Query text")
    query.add_argument("--db", default=str(DB_PATH))
    query.add_argument("--top", type=int, default=8)
    query.add_argument("--role", help="Optional squad role, e.g. pinchy, coral")
    query.add_argument("--skill", help="Optional skill filter")
    query.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    decide = subparsers.add_parser("decide", help="Suggest skills and memory for a task")
    decide.add_argument("text", help="Task or user request")
    decide.add_argument("--db", default=str(DB_PATH))
    decide.add_argument("--top", type=int, default=5)
    decide.add_argument("--role", help="Optional squad role, e.g. pinchy, coral")
    decide.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    pinchy = subparsers.add_parser("pinchy", help="Produce a Pinchy-style action plan")
    pinchy.add_argument("text", help="Task or user request")
    pinchy.add_argument("--db", default=str(DB_PATH))
    pinchy.add_argument("--top", type=int, default=5)
    pinchy.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    task_pack = subparsers.add_parser("task-pack", help="Resolve the best reusable task pack for a request")
    task_pack.add_argument("text", help="Task or user request")
    task_pack.add_argument("--db", default=str(DB_PATH))
    task_pack.add_argument("--top", type=int, default=5)
    task_pack.add_argument("--role", help="Optional squad role, e.g. pinchy, coral")
    task_pack.add_argument("--pack-id", help="Optional explicit task pack id override")
    task_pack.add_argument("--packs-file", default=str(TASK_PACKS_PATH))
    task_pack.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    execute_plan = subparsers.add_parser("execute-plan", help="Build an execution plan from the best task pack")
    execute_plan.add_argument("text", help="Task or user request")
    execute_plan.add_argument("--db", default=str(DB_PATH))
    execute_plan.add_argument("--top", type=int, default=5)
    execute_plan.add_argument("--role", help="Optional squad role, e.g. pinchy, coral")
    execute_plan.add_argument("--pack-id", help="Optional explicit task pack id override")
    execute_plan.add_argument("--packs-file", default=str(TASK_PACKS_PATH))
    execute_plan.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    complete_task = subparsers.add_parser("complete-task", help="Record the outcome of a completed task pack run")
    complete_task.add_argument("text", help="Original task or request text")
    complete_task.add_argument("--db", default=str(DB_PATH))
    complete_task.add_argument("--top", type=int, default=5)
    complete_task.add_argument("--role", help="Optional squad role, e.g. pinchy, coral")
    complete_task.add_argument("--pack-id", help="Optional explicit task pack id override")
    complete_task.add_argument("--packs-file", default=str(TASK_PACKS_PATH))
    complete_task.add_argument("--status", choices=["accepted", "revised", "failed"], required=True)
    complete_task.add_argument("--revision-count", type=int, default=0)
    complete_task.add_argument("--completion-minutes", type=float)
    complete_task.add_argument("--user-rating", type=float)
    complete_task.add_argument("--notes", default="")
    complete_task.add_argument("--used-path", action="append", dest="used_paths")
    complete_task.add_argument("--used-skill", action="append", dest="used_skills")
    complete_task.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    feedback = subparsers.add_parser("feedback", help="Record retrieval feedback for a query")
    feedback.add_argument("query", help="Original query text")
    feedback.add_argument("path", help="Path that was useful or not useful")
    feedback.add_argument("--db", default=str(DB_PATH))
    feedback.add_argument("--rating", choices=["useful", "not_useful"], required=True)

    logs = subparsers.add_parser("logs", help="Show recent query logs")
    logs.add_argument("--db", default=str(DB_PATH))
    logs.add_argument("--limit", type=int, default=20)

    train = subparsers.add_parser("train", help="Train learned path and skill priors from query logs and feedback")
    train.add_argument("--db", default=str(DB_PATH))
    train.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    report = subparsers.add_parser("report", help="Show usage-learning report for the squad memory index")
    report.add_argument("--db", default=str(DB_PATH))
    report.add_argument("--limit", type=int, default=10)
    report.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    pack_train = subparsers.add_parser("pack-train", help="Train learned pack priors from completed task outcomes")
    pack_train.add_argument("--db", default=str(DB_PATH))
    pack_train.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    pack_report = subparsers.add_parser("pack-report", help="Show pack outcome report with top packs, weak packs, and pack-level note patterns")
    pack_report.add_argument("--db", default=str(DB_PATH))
    pack_report.add_argument("--limit", type=int, default=10)
    pack_report.add_argument("--packs-file", default=str(TASK_PACKS_PATH))
    pack_report.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    suggest = subparsers.add_parser("suggest-metadata", help="Suggest frontmatter updates from usage patterns")
    suggest.add_argument("--db", default=str(DB_PATH))
    suggest.add_argument("--path", help="Optional single path to inspect")
    suggest.add_argument("--limit", type=int, default=8)
    suggest.add_argument("--min-useful", type=int, default=1)
    suggest.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    evaluate = subparsers.add_parser("eval", help="Run retrieval evaluation against fixture queries")
    evaluate.add_argument("--db", default=str(DB_PATH))
    evaluate.add_argument(
        "--fixtures",
        default=str(HOME / "squad_memory" / "evals" / "fixtures.json"),
        help="Path to evaluation fixture JSON",
    )
    evaluate.add_argument("--json", action="store_true", help="Emit JSON instead of text output")

    return parser.parse_args()


def tokenize(text: str) -> List[str]:
    return TOKEN_RE.findall(text.lower())


def slugify(text: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return value or "root"


def parse_frontmatter(text: str) -> Tuple[Dict[str, str], str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---", 4)
    if end == -1:
        return {}, text
    header = text[4:end].strip()
    body = text[end + 4 :].lstrip("\n")
    meta = {}
    for line in header.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        meta[key.strip()] = value.strip().strip('"')
    return meta, body


def doc_type_for(path: Path) -> str:
    if path.name == "SQUAD_MEMORY.md":
        return "squad_router"
    if path.name == "INDEX.md":
        return "memory_index"
    if path.name == "SKILL.md":
        return "skill_contract"
    if path.name == "MEMORY.md":
        return "memory_router"
    if path.parent.name == "references":
        return "reference_note"
    if path.parent.name == "memory":
        return "memory_note"
    return "skill_doc"


def parse_tags(meta: Dict[str, str]) -> List[str]:
    tags_value = meta.get("tags", "")
    if not tags_value:
        return []
    return [tag.strip().lower() for tag in tags_value.split(",") if tag.strip()]


def parse_meta_list(meta: Dict[str, str], *keys: str) -> List[str]:
    values: List[str] = []
    for key in keys:
        raw = meta.get(key, "")
        if not raw:
            continue
        values.extend(part.strip().lower() for part in raw.split(",") if part.strip())
    return sorted(set(values))


def parse_meta_bool(meta: Dict[str, str], key: str) -> bool:
    return meta.get(key, "").strip().lower() in {"1", "true", "yes", "y"}


def canonical_info_for_doc(rel_path: str, meta: Dict[str, str], canonical_map: Dict[str, str], topics: List[str], title: str) -> Tuple[bool, str]:
    canonical_group = canonical_map.get(rel_path, "").strip()
    if canonical_group:
        return True, canonical_group
    if parse_meta_bool(meta, "canonical"):
        meta_group = meta.get("canonical_group", "").strip()
        if meta_group:
            return True, meta_group
        if topics:
            return True, topics[0].replace("_", " ")
        if title:
            return True, title
        return True, Path(rel_path).stem.replace("-", " ")
    return False, ""


def top_sparse(weights: Dict[str, float], limit: int) -> Tuple[Dict[str, float], float]:
    if not weights:
        return {}, 1.0
    ranked = sorted(weights.items(), key=lambda item: abs(item[1]), reverse=True)[:limit]
    compact = {token: value for token, value in ranked}
    norm = math.sqrt(sum(value * value for value in compact.values())) or 1.0
    return compact, norm


def is_semantic_token(token: str, total_docs: int, doc_freq: Dict[str, int]) -> bool:
    if len(token) < 3 or token.isdigit():
        return False
    freq = int(doc_freq.get(token, 0))
    if freq < 2:
        return False
    max_df = max(int(total_docs * MAX_SEMANTIC_DF_RATIO), 25)
    if freq > max_df:
        return False
    return True


def expand_query(query: str, role: Optional[str] = None) -> Tuple[str, List[str], List[str]]:
    lowered = query.lower()
    original_tokens = set(tokenize(query))
    intents: List[str] = []
    expansions: List[str] = []

    for rule in QUERY_INTENT_RULES:
        if any(re.search(pattern, lowered) for pattern in rule["patterns"]):
            intents.append(rule["intent"])
            expansions.extend(rule["terms"])

    if role:
        expansions.extend(tokenize(role))

    ordered: List[str] = []
    seen = set(original_tokens)
    for term in expansions:
        if term in seen:
            continue
        seen.add(term)
        ordered.append(term)

    expanded_query = query
    if ordered:
        expanded_query = f"{query} {' '.join(ordered[:18])}"

    return expanded_query, sorted(set(intents)), ordered[:18]


def classify_heading(heading: str) -> str:
    value = heading.strip().lower()
    if value in {"document", "overview"}:
        return "overview"
    patterns = [
        ("quick_read", [r"quick read", r"key takeaways?"]),
        ("core_concept", [r"core concept", r"overview"]),
        ("data_stats", [r"data/stats", r"\bstats\b", r"data points", r"published"]),
        ("framework", [r"framework", r"method", r"workflow", r"playbook", r"checklist", r"output template"]),
        ("models_systems", [r"models?", r"systems?", r"internals?", r"mental models?"]),
        ("latest_posts", [r"latest relevant posts", r"archive", r"timeline"]),
        ("concepts", [r"concepts?", r"operating lens", r"heuristics?", r"how to use"]),
        ("examples", [r"examples?", r"case studies?"]),
        ("sources", [r"references?", r"sources?"]),
    ]
    for section_kind, regexes in patterns:
        if any(re.search(regex, value) for regex in regexes):
            return section_kind
    return "section"


def parse_date_text(value: str) -> Optional[date]:
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    try:
        return datetime.strptime(value, "%B %d, %Y").date()
    except ValueError:
        return None


def extract_published_on(meta: Dict[str, str], body: str) -> str:
    for key in ("updated_on", "updated", "published_on", "published", "date"):
        parsed = parse_date_text(meta.get(key, ""))
        if parsed:
            return parsed.isoformat()

    for line in body.splitlines()[:80]:
        line = line.strip()
        if not line:
            continue
        match = ISO_DATE_RE.search(line)
        if match:
            parsed = parse_date_text(match.group(1))
            if parsed:
                return parsed.isoformat()
        match = MONTH_DATE_RE.search(line)
        if match:
            parsed = parse_date_text(match.group(0))
            if parsed:
                return parsed.isoformat()
    return ""


def freshness_score(published_on: str) -> float:
    if not published_on:
        return 0.0
    parsed = parse_date_text(published_on)
    if not parsed:
        return 0.0
    days_old = max((date.today() - parsed).days, 0)
    if days_old <= 30:
        return 0.08
    if days_old <= 90:
        return 0.06
    if days_old <= 180:
        return 0.045
    if days_old <= 365:
        return 0.03
    if days_old <= 730:
        return 0.015
    return 0.0


def infer_source(skill: str, rel: Path) -> str:
    if skill == "dejan-ai-reverse-engineering" or any("dejan" in part for part in rel.parts):
        return "dejan"
    if rel.parts[0] == "seo" and rel.name.startswith("ahrefs-"):
        return "ahrefs"
    if rel.parts[0] == "seo" and rel.name.startswith("hobo-"):
        return "hobo"
    return ""


def parse_markdown_sections(text: str) -> List[Tuple[str, str]]:
    sections: List[Tuple[str, List[str]]] = []
    current_heading = "Document"
    current_lines: List[str] = []

    for line in text.splitlines():
        match = HEADING_RE.match(line)
        if match:
            if current_lines:
                sections.append((current_heading, current_lines))
            current_heading = match.group(2).strip()
            current_lines = [line]
        else:
            current_lines.append(line)

    if current_lines:
        sections.append((current_heading, current_lines))

    out: List[Tuple[str, str]] = []
    for heading, lines in sections:
        chunk_text = "\n".join(lines).strip()
        if chunk_text:
            out.append((heading, chunk_text))
    return out


def split_blocks(text: str) -> List[str]:
    blocks = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    out: List[str] = []
    for block in blocks:
        lines = [line.rstrip() for line in block.splitlines()]
        bullet_lines = [line for line in lines if re.match(r"^\s*[-*]\s+", line)]
        if len(bullet_lines) >= 6:
            group: List[str] = []
            for line in lines:
                if re.match(r"^\s*[-*]\s+", line):
                    group.append(line)
                    if len(group) >= 4:
                        out.append("\n".join(group))
                        group = []
                elif group:
                    group.append(line)
            if group:
                out.append("\n".join(group))
            continue
        out.append(block)
    return out


def skill_root_for_path(path: str) -> str:
    return path.split("/", 1)[0] if "/" in path else path


def normalize_list(value: object) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def load_task_packs(path: Path) -> List[TaskPack]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    packs: List[TaskPack] = []
    for item in payload.get("packs", []):
        packs.append(
            TaskPack(
                pack_id=str(item["id"]).strip(),
                name=str(item.get("name", item["id"])).strip(),
                description=str(item.get("description", "")).strip(),
                roles=[value.lower() for value in normalize_list(item.get("roles"))],
                intents=[value.lower() for value in normalize_list(item.get("intents"))],
                keywords=[value.lower() for value in normalize_list(item.get("keywords"))],
                primary_skill=str(item.get("primary_skill", "")).strip(),
                supporting_skills=normalize_list(item.get("supporting_skills")),
                memory_focus=[value.lower() for value in normalize_list(item.get("memory_focus"))],
                checklist=normalize_list(item.get("checklist")),
                deliverables=normalize_list(item.get("deliverables")),
                output_sections=normalize_list(item.get("output_sections")),
                handoffs=normalize_list(item.get("handoffs")),
                escalation_rules=normalize_list(item.get("escalation_rules")),
            )
        )
    return packs


def task_pack_to_dict(pack: TaskPack) -> dict:
    return {
        "id": pack.pack_id,
        "name": pack.name,
        "description": pack.description,
        "roles": pack.roles,
        "intents": pack.intents,
        "keywords": pack.keywords,
        "primary_skill": pack.primary_skill,
        "supporting_skills": pack.supporting_skills,
        "memory_focus": pack.memory_focus,
        "checklist": pack.checklist,
        "deliverables": pack.deliverables,
        "output_sections": pack.output_sections,
        "handoffs": pack.handoffs,
        "escalation_rules": pack.escalation_rules,
    }


def find_task_pack(packs: Sequence[TaskPack], pack_id: str) -> TaskPack:
    for pack in packs:
        if pack.pack_id == pack_id:
            return pack
    raise ValueError(f"Unknown task pack: {pack_id}")


def pack_focus_overlap(item: dict, pack: TaskPack) -> int:
    if not pack.memory_focus:
        return 0
    pack_terms = set(pack.memory_focus)
    meta_tokens = set(item.get("topics", []))
    meta_tokens.update(item.get("intents", []))
    meta_tokens.update(item.get("bundles", []))
    meta_tokens.update(tokenize(item.get("canonical_group", "")))
    meta_tokens.update(tokenize(item.get("heading", "")))
    meta_tokens.update(tokenize(item.get("path", "")))
    return len(meta_tokens & pack_terms)


def summarize_memory_themes(items: Sequence[dict], limit: int = 5) -> List[str]:
    themes: List[str] = []
    seen = set()
    for item in items:
        label = item.get("canonical_group") or item.get("heading") or skill_root_for_path(item.get("path", ""))
        label = label.strip()
        if not label or label in seen:
            continue
        seen.add(label)
        themes.append(label)
        if len(themes) >= limit:
            break
    return themes


def score_task_pack(pack: TaskPack, query: str, role: Optional[str], decision: dict) -> dict:
    lowered = query.lower()
    query_tokens = set(tokenize(query))
    inferred_intents = set(decision.get("inferred_intents", []))
    ranked_skills = [item["skill"] for item in decision.get("recommended_skills", [])]
    top_skills = ranked_skills[:5]
    top3_skills = set(ranked_skills[:3])
    score = 0.0
    reasons: List[str] = []

    if top_skills and pack.primary_skill == top_skills[0]:
        score += 2.2
        reasons.append(f"Primary skill matches top recommendation: {pack.primary_skill}")
    elif pack.primary_skill in top3_skills:
        score += 1.0
        reasons.append(f"Primary skill is already in top-3: {pack.primary_skill}")

    intent_hits = sorted(inferred_intents & set(pack.intents))
    if intent_hits:
        score += len(intent_hits) * 1.1
        reasons.append(f"Intent overlap: {', '.join(intent_hits)}")

    keyword_hits: List[str] = []
    for keyword in pack.keywords:
        if " " in keyword:
            if keyword in lowered:
                keyword_hits.append(keyword)
        elif keyword in query_tokens:
            keyword_hits.append(keyword)
    if keyword_hits:
        score += min(len(keyword_hits) * 0.35, 1.4)
        reasons.append(f"Keyword overlap: {', '.join(keyword_hits[:4])}")

    if role and role.lower() in pack.roles:
        score += 0.45
        reasons.append(f"Role match: {role.lower()}")

    support_hits: List[str] = []
    for skill in pack.supporting_skills:
        if skill in top3_skills:
            score += 0.45
            support_hits.append(skill)
        elif skill in top_skills:
            score += 0.2
            support_hits.append(skill)
    if support_hits:
        reasons.append(f"Supporting skills already surfaced: {', '.join(support_hits)}")

    memory_hits = 0
    for item in decision.get("supporting_memory", [])[:10]:
        memory_hits += pack_focus_overlap(item, pack)
    if memory_hits:
        score += min(memory_hits * 0.14, 0.9)
        reasons.append(f"Memory focus overlap score: {memory_hits}")

    return {
        "pack": pack,
        "score": round(score, 4),
        "reasons": reasons[:5],
    }


def usage_stat_row() -> Dict[str, int]:
    return {"useful_count": 0, "not_useful_count": 0, "exposure_count": 0}


def buckets_for_query(query: str, role: Optional[str] = None, inferred_intents: Optional[Sequence[str]] = None) -> List[str]:
    intents = sorted(set(inferred_intents or expand_query(query, role)[1]))
    return [GLOBAL_BUCKET] + intents


def usage_score(useful_count: int, not_useful_count: int, exposure_count: int) -> float:
    observed = max(exposure_count, useful_count + not_useful_count, 1)
    success_rate = useful_count / observed
    failure_rate = not_useful_count / observed
    score = useful_count * 0.22 - not_useful_count * 0.28 + success_rate * 0.45 - failure_rate * 0.35
    if exposure_count >= 3 and useful_count == 0:
        score -= min((exposure_count - 2) * 0.04, 0.2)
    return round(score, 4)


def pack_stat_row() -> Dict[str, float]:
    return {
        "accepted_count": 0,
        "revised_count": 0,
        "failed_count": 0,
        "exposure_count": 0,
        "rating_sum": 0.0,
        "rating_count": 0,
        "revision_sum": 0.0,
    }


def pack_buckets_for_query(query: str, role: Optional[str] = None) -> List[str]:
    buckets = buckets_for_query(query, role=role)
    if role:
        buckets.append(f"role:{role.lower()}")
    return list(dict.fromkeys(buckets))


def update_pack_stats(stats: Dict[str, float], status: str, revision_count: int, user_rating: Optional[float]) -> None:
    stats["exposure_count"] += 1
    field = f"{status}_count"
    if field in stats:
        stats[field] += 1
    stats["revision_sum"] += max(revision_count, 0)
    if user_rating is not None:
        stats["rating_sum"] += user_rating
        stats["rating_count"] += 1


def pack_outcome_score(
    accepted_count: int,
    revised_count: int,
    failed_count: int,
    exposure_count: int,
    avg_rating: Optional[float],
    avg_revisions: float,
) -> float:
    observed = max(exposure_count, accepted_count + revised_count + failed_count, 1)
    accepted_rate = accepted_count / observed
    revised_rate = revised_count / observed
    failed_rate = failed_count / observed
    rating_bonus = 0.0 if avg_rating is None else max(min((avg_rating - 3.0) * 0.08, 0.16), -0.16)
    revision_penalty = min(avg_revisions * 0.08, 0.24)
    score = (
        accepted_count * 0.24
        + revised_count * 0.04
        - failed_count * 0.32
        + accepted_rate * 0.55
        + revised_rate * 0.08
        - failed_rate * 0.5
        + rating_bonus
        - revision_penalty
    )
    if exposure_count >= 3 and accepted_count == 0:
        score -= min((exposure_count - 2) * 0.05, 0.25)
    return round(score, 4)


def extract_logged_paths_and_skills(mode: str, payload: dict) -> Tuple[List[str], List[str]]:
    paths: List[str] = []
    skills: List[str] = []

    def add_path(value: Optional[str]) -> None:
        if value:
            paths.append(value)
            skills.append(skill_root_for_path(value))

    def add_skill(value: Optional[str]) -> None:
        if value:
            skills.append(value)

    if mode == "query":
        for item in payload.get("results", []):
            add_path(item.get("path"))
            add_skill(item.get("skill"))
    elif mode == "decide":
        for item in payload.get("supporting_memory", []):
            add_path(item.get("path"))
        for item in payload.get("recommended_skills", []):
            add_skill(item.get("skill"))
    elif mode == "pinchy":
        for item in payload.get("memory_shortlist", []):
            add_path(item.get("path"))
        add_skill(payload.get("primary_skill"))
        for skill in payload.get("supporting_skills", []):
            add_skill(skill)
    elif mode in {"task-pack", "execute-plan"}:
        for item in payload.get("memory_shortlist", []):
            add_path(item.get("path"))
        selected_pack = payload.get("selected_pack", {})
        add_skill(selected_pack.get("primary_skill"))
        for skill in selected_pack.get("supporting_skills", []):
            add_skill(skill)
        for item in payload.get("recommended_skills", []):
            add_skill(item.get("skill"))

    return list(dict.fromkeys(paths)), list(dict.fromkeys(skills))


def chunk_section(
    path: Path,
    rel_path: str,
    skill: str,
    file_type: str,
    heading: str,
    text: str,
    tags: List[str],
    roles: List[str],
    topics: List[str],
    intents: List[str],
    use_for: List[str],
    avoid_for: List[str],
    confidence: str,
    is_canonical: bool,
    canonical_group: str,
    source: str,
    published_on: str,
    freshness: float,
) -> List[DocChunk]:
    paragraphs = split_blocks(text)
    chunks: List[DocChunk] = []
    buf: List[str] = []
    size = 0
    index = 0
    section_kind = classify_heading(heading)
    chunk_limit = 900 if file_type in {"memory_note", "reference_note"} else 1200
    chunk_tags = sorted(set(tags + [section_kind] + topics + intents + use_for))

    for para in paragraphs:
        if size + len(para) > chunk_limit and buf:
            body = "\n\n".join(buf)
            chunk_id = f"{rel_path}::{slugify(heading)}::{index}"
            chunks.append(
                DocChunk(
                    chunk_id=chunk_id,
                    path=rel_path,
                    skill=skill,
                    file_type=file_type,
                    heading=heading,
                    text=body,
                    section_kind=section_kind,
                    source=source,
                    published_on=published_on,
                    freshness=freshness,
                    topics=topics,
                    intents=intents,
                    use_for=use_for,
                    avoid_for=avoid_for,
                    confidence=confidence,
                    tags=chunk_tags,
                    roles=roles,
                    is_canonical=is_canonical,
                    canonical_group=canonical_group,
                )
            )
            index += 1
            buf = [para]
            size = len(para)
        else:
            buf.append(para)
            size += len(para)

    if buf:
        body = "\n\n".join(buf)
        chunk_id = f"{rel_path}::{slugify(heading)}::{index}"
        chunks.append(
            DocChunk(
                chunk_id=chunk_id,
                path=rel_path,
                skill=skill,
                file_type=file_type,
                heading=heading,
                text=body,
                section_kind=section_kind,
                source=source,
                published_on=published_on,
                freshness=freshness,
                topics=topics,
                intents=intents,
                use_for=use_for,
                avoid_for=avoid_for,
                confidence=confidence,
                tags=chunk_tags,
                roles=roles,
                is_canonical=is_canonical,
                canonical_group=canonical_group,
            )
        )
    return chunks


def chunk_index_text(chunk: DocChunk) -> str:
    return " ".join(
        part
        for part in [
            chunk.heading,
            chunk.text,
            " ".join(chunk.tags),
            " ".join(chunk.roles),
            " ".join(chunk.bundles),
            " ".join(chunk.topics),
            " ".join(chunk.intents),
            " ".join(chunk.use_for),
            chunk.section_kind,
            chunk.source,
            chunk.confidence,
        ]
        if part
    )


def parse_canonical_map(skills_root: Path) -> Dict[str, str]:
    path = skills_root / "seo" / "MEMORY.md"
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    result: Dict[str, str] = {}
    in_canonical = False
    for line in lines:
        line = line.strip()
        if line.startswith("### Canonical "):
            in_canonical = True
            continue
        if in_canonical and line.startswith("## "):
            break
        if not in_canonical:
            continue
        if line.startswith("- ") and ": `memory/" in line:
            title, rel = line[2:].split(": `", 1)
            rel_path = rel.rstrip("`")
            result[f"seo/{rel_path}"] = title.strip()
    return result


def parse_role_bundles(skills_root: Path) -> Dict[str, List[str]]:
    path = skills_root / "seo" / "MEMORY.md"
    if not path.exists():
        return {}

    bundles: Dict[str, List[str]] = defaultdict(list)
    lines = path.read_text(encoding="utf-8").splitlines()
    current_role: Optional[str] = None
    for raw in lines:
        line = raw.strip()
        if line.startswith("### ") and line.endswith(" Bundle"):
            current_role = line[4:-7].strip().lower()
            continue
        if current_role and line.startswith("- `memory/") and line.endswith("`"):
            bundles[current_role].append(f"seo/{line[3:-1]}")
    return bundles


def parse_bundle_memberships(skills_root: Path) -> Dict[str, List[str]]:
    path = skills_root / "seo" / "MEMORY.md"
    if not path.exists():
        return {}

    memberships: Dict[str, List[str]] = defaultdict(list)
    current_bundle: Optional[str] = None
    lines = path.read_text(encoding="utf-8").splitlines()
    for raw in lines:
        line = raw.strip()
        if line.startswith("### ") and line.endswith(" Bundle"):
            current_bundle = line[4:-7].strip().lower().replace(" ", "_")
            continue
        if current_bundle and line.startswith("- `memory/") and line.endswith("`"):
            rel_path = f"seo/{line[3:-1]}"
            memberships[rel_path].append(current_bundle)
    return memberships


def parse_squad_router(skills_root: Path) -> SquadRouter:
    path = skills_root / "SQUAD_MEMORY.md"
    if not path.exists():
        return SquadRouter(skill_tags={}, role_paths={}, path_bundles={})

    skill_tags: Dict[str, List[str]] = defaultdict(list)
    role_paths: Dict[str, List[str]] = defaultdict(list)
    path_bundles: Dict[str, List[str]] = defaultdict(list)

    mode: Optional[str] = None
    current_skill: Optional[str] = None
    current_role: Optional[str] = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line == "## Skill Tags":
            mode = "skill_tags"
            current_skill = None
            current_role = None
            continue
        if line == "## Role Bundles":
            mode = "role_bundles"
            current_skill = None
            current_role = None
            continue
        if mode == "skill_tags" and line.startswith("### `") and line.endswith("`"):
            current_skill = line[4:-1]
            continue
        if mode == "role_bundles" and line.startswith("### ") and line.endswith(" Bundle"):
            current_role = line[4:-7].strip().lower()
            continue
        if mode == "skill_tags" and current_skill and line.startswith("- tags: "):
            tags = [tag.strip().lower() for tag in line[8:].split(",") if tag.strip()]
            skill_tags[current_skill].extend(tags)
            continue
        if mode == "role_bundles" and current_role:
            if line.startswith("- `") and line.endswith("`"):
                rel_path = line[3:-1]
                role_paths[current_role].append(rel_path)
                path_bundles[rel_path].append(current_role)
                continue
            if line.startswith("- skill `") and line.endswith("`"):
                skill = line[9:-1]
                rel_path = f"{skill}/SKILL.md"
                role_paths[current_role].append(rel_path)
                path_bundles[rel_path].append(current_role)

    return SquadRouter(
        skill_tags={key: sorted(set(value)) for key, value in skill_tags.items()},
        role_paths={key: sorted(set(value)) for key, value in role_paths.items()},
        path_bundles={key: sorted(set(value)) for key, value in path_bundles.items()},
    )


def collect_chunks(skills_root: Path) -> Tuple[List[DocChunk], Dict[str, List[str]]]:
    canonical_map = parse_canonical_map(skills_root)
    seo_role_bundles = parse_role_bundles(skills_root)
    seo_bundle_memberships = parse_bundle_memberships(skills_root)
    squad_router = parse_squad_router(skills_root)
    role_bundles: Dict[str, List[str]] = defaultdict(list)
    for role, paths in seo_role_bundles.items():
        role_bundles[role].extend(paths)
    for role, paths in squad_router.role_paths.items():
        role_bundles[role].extend(paths)

    bundle_memberships: Dict[str, List[str]] = defaultdict(list)
    for rel_path, bundles in seo_bundle_memberships.items():
        bundle_memberships[rel_path].extend(bundles)
    for rel_path, bundles in squad_router.path_bundles.items():
        bundle_memberships[rel_path].extend(bundles)

    chunks: List[DocChunk] = []

    for path in sorted(skills_root.rglob("*.md")):
        rel = path.relative_to(skills_root)
        if any(part.startswith(".") and part != ".system" for part in rel.parts):
            continue
        skill = rel.parts[0] if len(rel.parts) > 1 else "squad_router"
        text = path.read_text(encoding="utf-8")
        meta, body = parse_frontmatter(text)
        file_type = doc_type_for(path)
        rel_path = str(rel)
        source = infer_source(skill, rel)
        published_on = extract_published_on(meta, body)
        freshness = freshness_score(published_on)
        tags = parse_tags(meta)
        topics = parse_meta_list(meta, "topic", "topics")
        intents = parse_meta_list(meta, "intent", "intents")
        use_for = parse_meta_list(meta, "use_for")
        avoid_for = parse_meta_list(meta, "avoid_for")
        explicit_roles = parse_meta_list(meta, "role", "roles")
        confidence = meta.get("confidence", "").strip().lower()
        is_canonical, canonical_group = canonical_info_for_doc(
            rel_path,
            meta,
            canonical_map,
            topics,
            meta.get("title", "").strip(),
        )
        tags.extend(squad_router.skill_tags.get(skill, []))
        tags.extend(tokenize(rel.stem.replace("-", " ")))
        tags.extend(tokenize(skill.replace("-", " ")))
        tags.extend(topics)
        tags.extend(intents)
        tags.extend(use_for)
        if confidence:
            tags.append(f"confidence_{confidence}")
        if file_type == "memory_note" and rel.parts[0] == "seo":
            tags.append("seo_memory")
        if file_type == "reference_note":
            tags.append("reference_note")
        if rel.parts[0] == "seo" and rel.parts[-1].startswith("ahrefs-"):
            tags.extend(["source_ahrefs", "ahrefs"])
        if rel.parts[0] == "seo" and rel.parts[-1].startswith("hobo-"):
            tags.extend(["source_hobo", "hobo"])
        if source == "dejan":
            tags.extend(["source_dejan", "dejan", "reverse_engineering"])
        if published_on:
            tags.extend(tokenize(published_on))
        tags = sorted(set(tags))
        bundles = sorted(set(bundle_memberships.get(rel_path, [])))
        roles = sorted(set(SKILL_ALIASES.get(skill, []) + explicit_roles))

        for heading, section_text in parse_markdown_sections(body):
            section_chunks = chunk_section(
                path,
                rel_path,
                skill,
                file_type,
                heading,
                section_text,
                tags,
                roles,
                topics,
                intents,
                use_for,
                avoid_for,
                confidence,
                is_canonical,
                canonical_group,
                source,
                published_on,
                freshness,
            )
            for chunk in section_chunks:
                chunk.bundles = bundles
            chunks.extend(section_chunks)

    return chunks, {role: sorted(set(paths)) for role, paths in role_bundles.items()}


def ensure_schema(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE chunks (
          chunk_id TEXT PRIMARY KEY,
          path TEXT NOT NULL,
          skill TEXT NOT NULL,
          file_type TEXT NOT NULL,
          heading TEXT NOT NULL,
          text TEXT NOT NULL,
          section_kind TEXT NOT NULL,
          source TEXT NOT NULL,
          published_on TEXT NOT NULL,
          freshness REAL NOT NULL,
          topics_json TEXT NOT NULL,
          intents_json TEXT NOT NULL,
          use_for_json TEXT NOT NULL,
          avoid_for_json TEXT NOT NULL,
          confidence TEXT NOT NULL,
          tags_json TEXT NOT NULL,
          roles_json TEXT NOT NULL,
          bundles_json TEXT NOT NULL,
          is_canonical INTEGER NOT NULL,
          canonical_group TEXT NOT NULL
        );

        CREATE TABLE chunk_weights (
          chunk_id TEXT PRIMARY KEY,
          weights_json TEXT NOT NULL,
          norm REAL NOT NULL
        );

        CREATE TABLE chunk_vectors (
          chunk_id TEXT PRIMARY KEY,
          weights_json TEXT NOT NULL,
          norm REAL NOT NULL
        );

        CREATE TABLE token_vectors (
          token TEXT PRIMARY KEY,
          weights_json TEXT NOT NULL,
          norm REAL NOT NULL
        );

        CREATE TABLE meta (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE role_bundles (
          role TEXT NOT NULL,
          path TEXT NOT NULL
        );

        CREATE TABLE query_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          mode TEXT NOT NULL,
          query TEXT NOT NULL,
          role TEXT,
          skill_filter TEXT,
          top_n INTEGER NOT NULL,
          result_json TEXT NOT NULL
        );

        CREATE TABLE feedback (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          query TEXT NOT NULL,
          path TEXT NOT NULL,
          rating TEXT NOT NULL
        );

        CREATE TABLE task_outcomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          query TEXT NOT NULL,
          role TEXT,
          pack_id TEXT NOT NULL,
          primary_skill TEXT NOT NULL,
          supporting_skills_json TEXT NOT NULL,
          used_skills_json TEXT NOT NULL,
          memory_paths_json TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('accepted', 'revised', 'failed')),
          revision_count INTEGER NOT NULL DEFAULT 0,
          completion_minutes REAL,
          user_rating REAL,
          notes TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE learned_path_priors (
          bucket TEXT NOT NULL,
          path TEXT NOT NULL,
          score REAL NOT NULL,
          useful_count INTEGER NOT NULL,
          not_useful_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(bucket, path)
        );

        CREATE TABLE learned_skill_priors (
          bucket TEXT NOT NULL,
          skill TEXT NOT NULL,
          score REAL NOT NULL,
          useful_count INTEGER NOT NULL,
          not_useful_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(bucket, skill)
        );

        CREATE TABLE learned_pack_priors (
          bucket TEXT NOT NULL,
          pack_id TEXT NOT NULL,
          score REAL NOT NULL,
          accepted_count INTEGER NOT NULL,
          revised_count INTEGER NOT NULL,
          failed_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(bucket, pack_id)
        );

        CREATE TABLE learned_pack_path_priors (
          pack_id TEXT NOT NULL,
          path TEXT NOT NULL,
          score REAL NOT NULL,
          accepted_count INTEGER NOT NULL,
          revised_count INTEGER NOT NULL,
          failed_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(pack_id, path)
        );

        CREATE TABLE learned_pack_skill_priors (
          pack_id TEXT NOT NULL,
          skill TEXT NOT NULL,
          score REAL NOT NULL,
          accepted_count INTEGER NOT NULL,
          revised_count INTEGER NOT NULL,
          failed_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(pack_id, skill)
        );

        CREATE TABLE training_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          query_logs INTEGER NOT NULL,
          feedback_rows INTEGER NOT NULL,
          path_priors INTEGER NOT NULL,
          skill_priors INTEGER NOT NULL
        );

        CREATE TABLE pack_training_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          task_outcomes INTEGER NOT NULL,
          pack_priors INTEGER NOT NULL,
          pack_path_priors INTEGER NOT NULL,
          pack_skill_priors INTEGER NOT NULL
        );

        CREATE VIRTUAL TABLE chunks_fts USING fts5(
          chunk_id UNINDEXED,
          path,
          heading,
          text,
          section_kind,
          source,
          topics,
          intents,
          use_for,
          confidence,
          tags,
          roles,
          bundles
        );
        """
    )


def ensure_learning_tables(con: sqlite3.Connection) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS task_outcomes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          query TEXT NOT NULL,
          role TEXT,
          pack_id TEXT NOT NULL,
          primary_skill TEXT NOT NULL,
          supporting_skills_json TEXT NOT NULL,
          used_skills_json TEXT NOT NULL,
          memory_paths_json TEXT NOT NULL,
          status TEXT NOT NULL CHECK(status IN ('accepted', 'revised', 'failed')),
          revision_count INTEGER NOT NULL DEFAULT 0,
          completion_minutes REAL,
          user_rating REAL,
          notes TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS learned_path_priors (
          bucket TEXT NOT NULL,
          path TEXT NOT NULL,
          score REAL NOT NULL,
          useful_count INTEGER NOT NULL,
          not_useful_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(bucket, path)
        );

        CREATE TABLE IF NOT EXISTS learned_skill_priors (
          bucket TEXT NOT NULL,
          skill TEXT NOT NULL,
          score REAL NOT NULL,
          useful_count INTEGER NOT NULL,
          not_useful_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(bucket, skill)
        );

        CREATE TABLE IF NOT EXISTS learned_pack_priors (
          bucket TEXT NOT NULL,
          pack_id TEXT NOT NULL,
          score REAL NOT NULL,
          accepted_count INTEGER NOT NULL,
          revised_count INTEGER NOT NULL,
          failed_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(bucket, pack_id)
        );

        CREATE TABLE IF NOT EXISTS learned_pack_path_priors (
          pack_id TEXT NOT NULL,
          path TEXT NOT NULL,
          score REAL NOT NULL,
          accepted_count INTEGER NOT NULL,
          revised_count INTEGER NOT NULL,
          failed_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(pack_id, path)
        );

        CREATE TABLE IF NOT EXISTS learned_pack_skill_priors (
          pack_id TEXT NOT NULL,
          skill TEXT NOT NULL,
          score REAL NOT NULL,
          accepted_count INTEGER NOT NULL,
          revised_count INTEGER NOT NULL,
          failed_count INTEGER NOT NULL,
          exposure_count INTEGER NOT NULL,
          PRIMARY KEY(pack_id, skill)
        );

        CREATE TABLE IF NOT EXISTS training_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          query_logs INTEGER NOT NULL,
          feedback_rows INTEGER NOT NULL,
          path_priors INTEGER NOT NULL,
          skill_priors INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pack_training_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          task_outcomes INTEGER NOT NULL,
          pack_priors INTEGER NOT NULL,
          pack_path_priors INTEGER NOT NULL,
          pack_skill_priors INTEGER NOT NULL
        );
        """
    )


def copy_usage_tables(old_db_path: Path, con: sqlite3.Connection) -> None:
    if not old_db_path.exists():
        return

    old = sqlite3.connect(str(old_db_path))
    try:
        tables = {row[0] for row in old.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "query_log" in tables:
            for row in old.execute("SELECT id, ts, mode, query, role, skill_filter, top_n, result_json FROM query_log ORDER BY id"):
                con.execute(
                    "INSERT INTO query_log(id, ts, mode, query, role, skill_filter, top_n, result_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    row,
                )
        if "feedback" in tables:
            for row in old.execute("SELECT id, ts, query, path, rating FROM feedback ORDER BY id"):
                con.execute(
                    "INSERT INTO feedback(id, ts, query, path, rating) VALUES (?, ?, ?, ?, ?)",
                    row,
                )
        if "task_outcomes" in tables:
            for row in old.execute(
                """
                SELECT id, ts, query, role, pack_id, primary_skill, supporting_skills_json, used_skills_json,
                       memory_paths_json, status, revision_count, completion_minutes, user_rating, notes
                FROM task_outcomes
                ORDER BY id
                """
            ):
                con.execute(
                    """
                    INSERT INTO task_outcomes(
                      id, ts, query, role, pack_id, primary_skill, supporting_skills_json, used_skills_json,
                      memory_paths_json, status, revision_count, completion_minutes, user_rating, notes
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        if "training_runs" in tables:
            for row in old.execute("SELECT id, ts, query_logs, feedback_rows, path_priors, skill_priors FROM training_runs ORDER BY id"):
                con.execute(
                    "INSERT INTO training_runs(id, ts, query_logs, feedback_rows, path_priors, skill_priors) VALUES (?, ?, ?, ?, ?, ?)",
                    row,
                )
        if "pack_training_runs" in tables:
            for row in old.execute(
                "SELECT id, ts, task_outcomes, pack_priors, pack_path_priors, pack_skill_priors FROM pack_training_runs ORDER BY id"
            ):
                con.execute(
                    """
                    INSERT INTO pack_training_runs(id, ts, task_outcomes, pack_priors, pack_path_priors, pack_skill_priors)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        if "learned_pack_priors" in tables:
            for row in old.execute(
                """
                SELECT bucket, pack_id, score, accepted_count, revised_count, failed_count, exposure_count
                FROM learned_pack_priors
                ORDER BY bucket, pack_id
                """
            ):
                con.execute(
                    """
                    INSERT INTO learned_pack_priors(bucket, pack_id, score, accepted_count, revised_count, failed_count, exposure_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        if "learned_pack_path_priors" in tables:
            for row in old.execute(
                """
                SELECT pack_id, path, score, accepted_count, revised_count, failed_count, exposure_count
                FROM learned_pack_path_priors
                ORDER BY pack_id, path
                """
            ):
                con.execute(
                    """
                    INSERT INTO learned_pack_path_priors(pack_id, path, score, accepted_count, revised_count, failed_count, exposure_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        if "learned_pack_skill_priors" in tables:
            for row in old.execute(
                """
                SELECT pack_id, skill, score, accepted_count, revised_count, failed_count, exposure_count
                FROM learned_pack_skill_priors
                ORDER BY pack_id, skill
                """
            ):
                con.execute(
                    """
                    INSERT INTO learned_pack_skill_priors(pack_id, skill, score, accepted_count, revised_count, failed_count, exposure_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
    finally:
        old.close()


def build_index(skills_root: Path, db_path: Path) -> None:
    chunks, role_bundles = collect_chunks(skills_root)
    doc_freq: Dict[str, int] = Counter()
    tokenized: Dict[str, Counter] = {}

    for chunk in chunks:
        counts = Counter(tokenize(chunk_index_text(chunk)))
        tokenized[chunk.chunk_id] = counts
        for token in counts:
            doc_freq[token] += 1

    total_docs = max(len(chunks), 1)
    token_semantic_vectors = build_token_semantic_vectors(tokenized, doc_freq, total_docs)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    temp_db_path = db_path.with_suffix(db_path.suffix + ".tmp")
    if temp_db_path.exists():
        temp_db_path.unlink()

    con = sqlite3.connect(str(temp_db_path))
    ensure_schema(con)
    copy_usage_tables(db_path, con)

    for chunk in chunks:
        counts = tokenized[chunk.chunk_id]
        weights = {}
        norm_sq = 0.0
        total_terms = sum(counts.values()) or 1
        for token, tf_count in counts.items():
            tf = tf_count / total_terms
            idf = math.log(1 + (total_docs / (1 + doc_freq[token])))
            weight = tf * idf
            weights[token] = weight
            norm_sq += weight * weight

        con.execute(
            """
            INSERT INTO chunks(
              chunk_id, path, skill, file_type, heading, text, section_kind, source, published_on, freshness,
              topics_json, intents_json, use_for_json, avoid_for_json, confidence,
              tags_json, roles_json, bundles_json, is_canonical, canonical_group
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chunk.chunk_id,
                chunk.path,
                chunk.skill,
                chunk.file_type,
                chunk.heading,
                chunk.text,
                chunk.section_kind,
                chunk.source,
                chunk.published_on,
                chunk.freshness,
                json.dumps(chunk.topics),
                json.dumps(chunk.intents),
                json.dumps(chunk.use_for),
                json.dumps(chunk.avoid_for),
                chunk.confidence,
                json.dumps(chunk.tags),
                json.dumps(chunk.roles),
                json.dumps(chunk.bundles),
                1 if chunk.is_canonical else 0,
                chunk.canonical_group,
            ),
        )
        con.execute(
            "INSERT INTO chunk_weights(chunk_id, weights_json, norm) VALUES (?, ?, ?)",
            (chunk.chunk_id, json.dumps(weights), math.sqrt(norm_sq) or 1.0),
        )
        vector_weights, vector_norm = semantic_vector_from_counts(counts, token_semantic_vectors, total_terms=total_terms)
        con.execute(
            "INSERT INTO chunk_vectors(chunk_id, weights_json, norm) VALUES (?, ?, ?)",
            (chunk.chunk_id, json.dumps(vector_weights), vector_norm),
        )
        con.execute(
            """
            INSERT INTO chunks_fts(chunk_id, path, heading, text, section_kind, source, topics, intents, use_for, confidence, tags, roles, bundles)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                chunk.chunk_id,
                chunk.path,
                chunk.heading,
                chunk.text,
                chunk.section_kind,
                chunk.source,
                " ".join(chunk.topics),
                " ".join(chunk.intents),
                " ".join(chunk.use_for),
                chunk.confidence,
                " ".join(chunk.tags),
                " ".join(chunk.roles),
                " ".join(chunk.bundles),
            ),
        )

    for token, (weights, norm) in token_semantic_vectors.items():
        con.execute(
            "INSERT INTO token_vectors(token, weights_json, norm) VALUES (?, ?, ?)",
            (token, json.dumps(weights), norm),
        )

    for role, paths in role_bundles.items():
        for path in paths:
            con.execute("INSERT INTO role_bundles(role, path) VALUES (?, ?)", (role, path))

    con.execute("INSERT INTO meta(key, value) VALUES (?, ?)", ("total_docs", str(total_docs)))
    con.execute("INSERT INTO meta(key, value) VALUES (?, ?)", ("doc_freq", json.dumps(doc_freq)))
    train_summary = train_usage_priors_in_connection(con)
    pack_summary = train_pack_priors_in_connection(con)
    train_summary = {**train_summary, **pack_summary}
    con.commit()
    con.close()
    os.replace(temp_db_path, db_path)

    print(
        f"Built index with {len(chunks)} chunks into {db_path} "
        f"(trained {train_summary['path_priors']} path priors, {train_summary['skill_priors']} skill priors, "
        f"{train_summary['pack_priors']} pack priors)"
    )


def load_meta(con: sqlite3.Connection) -> Tuple[int, Dict[str, int]]:
    rows = dict(con.execute("SELECT key, value FROM meta").fetchall())
    total_docs = int(rows["total_docs"])
    doc_freq = json.loads(rows["doc_freq"])
    return total_docs, doc_freq


def query_weights(query: str, total_docs: int, doc_freq: Dict[str, int]) -> Tuple[Dict[str, float], float]:
    counts = Counter(tokenize(query))
    total_terms = sum(counts.values()) or 1
    weights = {}
    norm_sq = 0.0
    for token, tf_count in counts.items():
        tf = tf_count / total_terms
        idf = math.log(1 + (total_docs / (1 + int(doc_freq.get(token, 0)))))
        weight = tf * idf
        weights[token] = weight
        norm_sq += weight * weight
    return weights, math.sqrt(norm_sq) or 1.0


def sparse_cosine(query_w: Dict[str, float], query_norm: float, doc_w: Dict[str, float], doc_norm: float) -> float:
    dot = 0.0
    for token, weight in query_w.items():
        dot += weight * doc_w.get(token, 0.0)
    return dot / (query_norm * doc_norm) if dot else 0.0


def build_token_semantic_vectors(tokenized: Dict[str, Counter], doc_freq: Dict[str, int], total_docs: int) -> Dict[str, Tuple[Dict[str, float], float]]:
    token_contexts: Dict[str, Counter] = defaultdict(Counter)

    for counts in tokenized.values():
        semantic_tokens = [
            token
            for token, _count in counts.most_common(48)
            if is_semantic_token(token, total_docs, doc_freq)
        ]
        if len(semantic_tokens) < 2:
            continue
        for token in semantic_tokens:
            token_count = counts[token]
            for other in semantic_tokens:
                if other == token:
                    continue
                token_contexts[token][other] += min(token_count, counts[other])

    vectors: Dict[str, Tuple[Dict[str, float], float]] = {}
    for token, contexts in token_contexts.items():
        weighted: Dict[str, float] = {}
        for context, count in contexts.most_common(DEFAULT_TOKEN_CONTEXT_LIMIT):
            idf = math.log(1 + (total_docs / (1 + int(doc_freq.get(context, 0)))))
            weighted[context] = math.log1p(count) * idf
        compact, norm = top_sparse(weighted, DEFAULT_TOKEN_CONTEXT_LIMIT)
        if compact:
            vectors[token] = (compact, norm)
    return vectors


def semantic_vector_from_counts(
    counts: Counter,
    token_vectors: Dict[str, Tuple[Dict[str, float], float]],
    total_terms: Optional[int] = None,
    limit: int = DEFAULT_VECTOR_DIM_LIMIT,
) -> Tuple[Dict[str, float], float]:
    if total_terms is None:
        total_terms = sum(counts.values()) or 1

    aggregate: Dict[str, float] = defaultdict(float)
    for token, token_count in counts.items():
        vector_row = token_vectors.get(token)
        if not vector_row:
            continue
        token_weights, _norm = vector_row
        multiplier = token_count / total_terms
        for dim, weight in token_weights.items():
            aggregate[dim] += multiplier * weight
    return top_sparse(dict(aggregate), limit)


def fetch_role_paths(con: sqlite3.Connection, role: Optional[str]) -> set:
    if not role:
        return set()
    rows = con.execute("SELECT path FROM role_bundles WHERE role = ?", (role.lower(),)).fetchall()
    return {row[0] for row in rows}


def fetch_path_feedback(con: sqlite3.Connection) -> Dict[str, float]:
    rows = con.execute(
        """
        SELECT path,
               SUM(CASE WHEN rating='useful' THEN 1 ELSE -1 END) AS score
        FROM feedback
        GROUP BY path
        """
    ).fetchall()
    return {path: float(score) for path, score in rows}


def skill_feedback_scores(con: sqlite3.Connection) -> Dict[str, float]:
    rows = con.execute(
        """
        SELECT substr(path, 1, instr(path, '/') - 1) AS skill_root,
               SUM(CASE WHEN rating='useful' THEN 1 ELSE -1 END) AS score
        FROM feedback
        WHERE instr(path, '/') > 0
        GROUP BY skill_root
        """
    ).fetchall()
    return {skill_root: float(score) for skill_root, score in rows}


def fetch_learned_path_priors(con: sqlite3.Connection, inferred_intents: Sequence[str]) -> Dict[str, float]:
    buckets = [GLOBAL_BUCKET] + sorted(set(inferred_intents))
    placeholders = ", ".join("?" for _ in buckets)
    rows = con.execute(
        f"SELECT bucket, path, score FROM learned_path_priors WHERE bucket IN ({placeholders})",
        buckets,
    ).fetchall()
    scores: Dict[str, float] = defaultdict(float)
    for bucket, path, score in rows:
        scores[path] += float(score) * (0.6 if bucket == GLOBAL_BUCKET else 1.0)
    return dict(scores)


def fetch_learned_skill_priors(con: sqlite3.Connection, inferred_intents: Sequence[str]) -> Dict[str, float]:
    buckets = [GLOBAL_BUCKET] + sorted(set(inferred_intents))
    placeholders = ", ".join("?" for _ in buckets)
    rows = con.execute(
        f"SELECT bucket, skill, score FROM learned_skill_priors WHERE bucket IN ({placeholders})",
        buckets,
    ).fetchall()
    scores: Dict[str, float] = defaultdict(float)
    for bucket, skill, score in rows:
        scores[skill] += float(score) * (0.6 if bucket == GLOBAL_BUCKET else 1.0)
    return dict(scores)


def fetch_learned_pack_priors(con: sqlite3.Connection, inferred_intents: Sequence[str], role: Optional[str]) -> Dict[str, float]:
    buckets = [GLOBAL_BUCKET] + sorted(set(inferred_intents))
    if role:
        buckets.append(f"role:{role.lower()}")
    placeholders = ", ".join("?" for _ in buckets)
    rows = con.execute(
        f"SELECT bucket, pack_id, score FROM learned_pack_priors WHERE bucket IN ({placeholders})",
        buckets,
    ).fetchall()
    scores: Dict[str, float] = defaultdict(float)
    for bucket, pack_id, score in rows:
        weight = 0.6 if bucket == GLOBAL_BUCKET else 1.0
        if role and bucket == f"role:{role.lower()}":
            weight = 1.1
        scores[pack_id] += float(score) * weight
    return dict(scores)


def fetch_pack_path_priors(con: sqlite3.Connection, pack_id: str) -> Dict[str, float]:
    rows = con.execute(
        """
        SELECT path, score
        FROM learned_pack_path_priors
        WHERE pack_id = ?
        """,
        (pack_id,),
    ).fetchall()
    return {path: float(score) for path, score in rows}


def fetch_pack_skill_priors(con: sqlite3.Connection, pack_id: str) -> Dict[str, float]:
    rows = con.execute(
        """
        SELECT skill, score
        FROM learned_pack_skill_priors
        WHERE pack_id = ?
        """,
        (pack_id,),
    ).fetchall()
    return {skill: float(score) for skill, score in rows}


def train_pack_priors_in_connection(con: sqlite3.Connection) -> dict:
    pack_stats: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(pack_stat_row)
    pack_path_stats: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(pack_stat_row)
    pack_skill_stats: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(pack_stat_row)

    rows = con.execute(
        """
        SELECT query, role, pack_id, used_skills_json, memory_paths_json, status, revision_count, user_rating
        FROM task_outcomes
        ORDER BY id
        """
    ).fetchall()

    con.execute("DELETE FROM learned_pack_priors")
    con.execute("DELETE FROM learned_pack_path_priors")
    con.execute("DELETE FROM learned_pack_skill_priors")

    for query, role, pack_id, used_skills_json, memory_paths_json, status, revision_count, user_rating in rows:
        try:
            used_skills = json.loads(used_skills_json or "[]")
            if not isinstance(used_skills, list):
                used_skills = []
        except json.JSONDecodeError:
            used_skills = []
        try:
            memory_paths = json.loads(memory_paths_json or "[]")
            if not isinstance(memory_paths, list):
                memory_paths = []
        except json.JSONDecodeError:
            memory_paths = []

        rating = float(user_rating) if user_rating is not None else None
        revision_count = int(revision_count or 0)

        for bucket in pack_buckets_for_query(query, role=role):
            update_pack_stats(pack_stats[(bucket, pack_id)], status, revision_count, rating)

        for path in list(dict.fromkeys(str(item) for item in memory_paths if str(item).strip())):
            update_pack_stats(pack_path_stats[(pack_id, path)], status, revision_count, rating)

        for skill in list(dict.fromkeys(str(item) for item in used_skills if str(item).strip())):
            update_pack_stats(pack_skill_stats[(pack_id, skill)], status, revision_count, rating)

    pack_rows = 0
    for (bucket, pack_id), stats in sorted(pack_stats.items()):
        avg_rating = stats["rating_sum"] / stats["rating_count"] if stats["rating_count"] else None
        avg_revisions = stats["revision_sum"] / max(stats["exposure_count"], 1)
        score = pack_outcome_score(
            int(stats["accepted_count"]),
            int(stats["revised_count"]),
            int(stats["failed_count"]),
            int(stats["exposure_count"]),
            avg_rating,
            avg_revisions,
        )
        con.execute(
            """
            INSERT INTO learned_pack_priors(bucket, pack_id, score, accepted_count, revised_count, failed_count, exposure_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bucket,
                pack_id,
                score,
                int(stats["accepted_count"]),
                int(stats["revised_count"]),
                int(stats["failed_count"]),
                int(stats["exposure_count"]),
            ),
        )
        pack_rows += 1

    pack_path_rows = 0
    for (pack_id, path), stats in sorted(pack_path_stats.items()):
        avg_rating = stats["rating_sum"] / stats["rating_count"] if stats["rating_count"] else None
        avg_revisions = stats["revision_sum"] / max(stats["exposure_count"], 1)
        score = pack_outcome_score(
            int(stats["accepted_count"]),
            int(stats["revised_count"]),
            int(stats["failed_count"]),
            int(stats["exposure_count"]),
            avg_rating,
            avg_revisions,
        )
        con.execute(
            """
            INSERT INTO learned_pack_path_priors(pack_id, path, score, accepted_count, revised_count, failed_count, exposure_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pack_id,
                path,
                score,
                int(stats["accepted_count"]),
                int(stats["revised_count"]),
                int(stats["failed_count"]),
                int(stats["exposure_count"]),
            ),
        )
        pack_path_rows += 1

    pack_skill_rows = 0
    for (pack_id, skill), stats in sorted(pack_skill_stats.items()):
        avg_rating = stats["rating_sum"] / stats["rating_count"] if stats["rating_count"] else None
        avg_revisions = stats["revision_sum"] / max(stats["exposure_count"], 1)
        score = pack_outcome_score(
            int(stats["accepted_count"]),
            int(stats["revised_count"]),
            int(stats["failed_count"]),
            int(stats["exposure_count"]),
            avg_rating,
            avg_revisions,
        )
        con.execute(
            """
            INSERT INTO learned_pack_skill_priors(pack_id, skill, score, accepted_count, revised_count, failed_count, exposure_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pack_id,
                skill,
                score,
                int(stats["accepted_count"]),
                int(stats["revised_count"]),
                int(stats["failed_count"]),
                int(stats["exposure_count"]),
            ),
        )
        pack_skill_rows += 1

    con.execute(
        """
        INSERT INTO pack_training_runs(task_outcomes, pack_priors, pack_path_priors, pack_skill_priors)
        VALUES (?, ?, ?, ?)
        """,
        (len(rows), pack_rows, pack_path_rows, pack_skill_rows),
    )

    return {
        "task_outcomes": len(rows),
        "pack_priors": pack_rows,
        "pack_path_priors": pack_path_rows,
        "pack_skill_priors": pack_skill_rows,
    }


def train_usage_priors_in_connection(con: sqlite3.Connection) -> dict:
    path_stats: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(usage_stat_row)
    skill_stats: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(usage_stat_row)

    log_rows = con.execute("SELECT mode, query, role, result_json FROM query_log ORDER BY id").fetchall()
    feedback_rows = con.execute("SELECT query, path, rating FROM feedback ORDER BY id").fetchall()

    con.execute("DELETE FROM learned_path_priors")
    con.execute("DELETE FROM learned_skill_priors")

    for mode, query, role, result_json in log_rows:
        try:
            payload = json.loads(result_json)
        except json.JSONDecodeError:
            continue
        inferred_intents = payload.get("inferred_intents") if isinstance(payload, dict) else None
        buckets = buckets_for_query(query, role=role, inferred_intents=inferred_intents)
        paths, skills = extract_logged_paths_and_skills(mode, payload if isinstance(payload, dict) else {})
        for bucket in buckets:
            for path in paths:
                path_stats[(bucket, path)]["exposure_count"] += 1
            for skill in skills:
                skill_stats[(bucket, skill)]["exposure_count"] += 1

    for query, path, rating in feedback_rows:
        buckets = buckets_for_query(query)
        skill = skill_root_for_path(path)
        target_field = "useful_count" if rating == "useful" else "not_useful_count"
        for bucket in buckets:
            path_stats[(bucket, path)][target_field] += 1
            skill_stats[(bucket, skill)][target_field] += 1

    path_rows = 0
    for (bucket, path), stats in sorted(path_stats.items()):
        score = usage_score(stats["useful_count"], stats["not_useful_count"], stats["exposure_count"])
        con.execute(
            """
            INSERT INTO learned_path_priors(bucket, path, score, useful_count, not_useful_count, exposure_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                bucket,
                path,
                score,
                stats["useful_count"],
                stats["not_useful_count"],
                stats["exposure_count"],
            ),
        )
        path_rows += 1

    skill_rows = 0
    for (bucket, skill), stats in sorted(skill_stats.items()):
        score = usage_score(stats["useful_count"], stats["not_useful_count"], stats["exposure_count"])
        con.execute(
            """
            INSERT INTO learned_skill_priors(bucket, skill, score, useful_count, not_useful_count, exposure_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                bucket,
                skill,
                score,
                stats["useful_count"],
                stats["not_useful_count"],
                stats["exposure_count"],
            ),
        )
        skill_rows += 1

    con.execute(
        """
        INSERT INTO training_runs(query_logs, feedback_rows, path_priors, skill_priors)
        VALUES (?, ?, ?, ?)
        """,
        (len(log_rows), len(feedback_rows), path_rows, skill_rows),
    )

    return {
        "query_logs": len(log_rows),
        "feedback_rows": len(feedback_rows),
        "path_priors": path_rows,
        "skill_priors": skill_rows,
    }


def train_usage_priors(db_path: Path) -> dict:
    con = sqlite3.connect(str(db_path))
    try:
        ensure_learning_tables(con)
        summary = train_usage_priors_in_connection(con)
        pack_summary = train_pack_priors_in_connection(con)
        con.commit()
        return {**summary, **pack_summary}
    finally:
        con.close()


def feedback_aware_top(results: List[dict], top: int) -> List[dict]:
    pool = sorted(results, key=lambda item: item["score"], reverse=True)
    selected: List[dict] = []
    path_counts: Counter = Counter()
    canonical_counts: Counter = Counter()
    source_counts: Counter = Counter()

    while pool and len(selected) < top:
        best_index = 0
        best_score = None
        for index, item in enumerate(pool):
            adjusted = item["score"]
            adjusted -= path_counts[item["path"]] * 0.85
            if item["canonical_group"]:
                adjusted -= canonical_counts[item["canonical_group"]] * 0.25
            if item["source"]:
                adjusted -= source_counts[item["source"]] * 0.08
            if item["file_type"] == "memory_index":
                adjusted -= 0.45
            elif item["file_type"] in {"memory_router", "squad_router", "skill_contract"}:
                adjusted -= 0.25
            if best_score is None or adjusted > best_score:
                best_score = adjusted
                best_index = index
        chosen = pool.pop(best_index)
        selected.append(chosen)
        path_counts[chosen["path"]] += 1
        if chosen["canonical_group"]:
            canonical_counts[chosen["canonical_group"]] += 1
        if chosen["source"]:
            source_counts[chosen["source"]] += 1
    return selected


def fts_candidates(con: sqlite3.Connection, query: str, top_k: int = 40) -> List[Tuple[str, float]]:
    tokens = tokenize(query)
    if not tokens:
        return []
    fts_query = " OR ".join(f'"{token.replace(chr(34), "")}"' for token in tokens[:16])
    rows = con.execute(
        """
        SELECT chunk_id, bm25(chunks_fts) AS rank
        FROM chunks_fts
        WHERE chunks_fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """,
        (fts_query, top_k),
    ).fetchall()
    return [(chunk_id, -rank) for chunk_id, rank in rows]


def query_semantic_vector(con: sqlite3.Connection, query: str) -> Tuple[Dict[str, float], float]:
    counts = Counter(tokenize(query))
    return semantic_vector_from_counts(counts, load_token_vectors(con), total_terms=sum(counts.values()) or 1)


def load_token_vectors(con: sqlite3.Connection) -> Dict[str, Tuple[Dict[str, float], float]]:
    rows = con.execute("SELECT token, weights_json, norm FROM token_vectors").fetchall()
    return {
        token: (json.loads(weights_json), norm)
        for token, weights_json, norm in rows
    }


def semantic_candidates(con: sqlite3.Connection, query_weights: Dict[str, float], query_norm: float, top_k: int = 40) -> List[Tuple[str, float]]:
    if not query_weights:
        return []
    scored: List[Tuple[str, float]] = []
    rows = con.execute("SELECT chunk_id, weights_json, norm FROM chunk_vectors").fetchall()
    for chunk_id, weights_json, norm in rows:
        weights = json.loads(weights_json)
        score = sparse_cosine(query_weights, query_norm, weights, norm)
        if score > 0:
            scored.append((chunk_id, score))
    scored.sort(key=lambda item: item[1], reverse=True)
    return scored[:top_k]


def rank_chunks(db_path: Path, query: str, role: Optional[str], skill_filter: Optional[str], top: int) -> List[dict]:
    con = sqlite3.connect(str(db_path))
    ensure_learning_tables(con)
    expanded_query, inferred_intents, expansion_terms = expand_query(query, role)
    total_docs, doc_freq = load_meta(con)
    q_weights, q_norm = query_weights(expanded_query, total_docs, doc_freq)
    q_vector_weights, q_vector_norm = query_semantic_vector(con, expanded_query)
    role_paths = fetch_role_paths(con, role)
    feedback_scores = fetch_path_feedback(con)
    skill_feedback = skill_feedback_scores(con)
    learned_path_priors = fetch_learned_path_priors(con, inferred_intents)
    query_token_set = set(tokenize(expanded_query))
    lexical_candidates = fts_candidates(con, expanded_query, top_k=max(top * 12, 60))
    vector_candidates = semantic_candidates(con, q_vector_weights, q_vector_norm, top_k=max(top * 10, 50))
    candidate_scores: Dict[str, Dict[str, float]] = defaultdict(lambda: {"lexical": 0.0, "vector_seed": 0.0, "learned_seed": 0.0})
    for chunk_id, lexical_score in lexical_candidates:
        candidate_scores[chunk_id]["lexical"] = max(candidate_scores[chunk_id]["lexical"], lexical_score)
    for chunk_id, vector_score in vector_candidates:
        candidate_scores[chunk_id]["vector_seed"] = max(candidate_scores[chunk_id]["vector_seed"], vector_score)
    for path, score in sorted(learned_path_priors.items(), key=lambda item: item[1], reverse=True)[: max(top * 6, 24)]:
        if score <= 0:
            continue
        rows = con.execute(
            """
            SELECT chunk_id
            FROM chunks
            WHERE path = ?
            ORDER BY is_canonical DESC, freshness DESC
            LIMIT 4
            """,
            (path,),
        ).fetchall()
        for (chunk_id,) in rows:
            candidate_scores[chunk_id]["learned_seed"] = max(candidate_scores[chunk_id]["learned_seed"], score)
    candidates = list(candidate_scores.items())

    # Fallback if FTS returns too little.
    if not candidates:
        rows = con.execute("SELECT chunk_id FROM chunks LIMIT 100").fetchall()
        candidates = [(row[0], {"lexical": 0.0, "vector_seed": 0.0, "learned_seed": 0.0}) for row in rows]

    ranked = []
    for chunk_id, seed_scores in candidates:
        row = con.execute(
            """
            SELECT c.path, c.skill, c.file_type, c.heading, c.text, c.section_kind, c.source, c.published_on, c.freshness,
                   c.topics_json, c.intents_json, c.use_for_json, c.avoid_for_json, c.confidence,
                   c.tags_json, c.roles_json, c.bundles_json, c.is_canonical, c.canonical_group,
                   w.weights_json, w.norm, v.weights_json, v.norm
            FROM chunks c
            JOIN chunk_weights w ON w.chunk_id = c.chunk_id
            JOIN chunk_vectors v ON v.chunk_id = c.chunk_id
            WHERE c.chunk_id = ?
            """,
            (chunk_id,),
        ).fetchone()
        if not row:
            continue
        (
            path,
            skill,
            file_type,
            heading,
            text,
            section_kind,
            source,
            published_on,
            freshness,
            topics_json,
            intents_json,
            use_for_json,
            avoid_for_json,
            confidence,
            tags_json,
            roles_json,
            bundles_json,
            is_canonical,
            canonical_group,
            weights_json,
            norm,
            vector_weights_json,
            vector_norm,
        ) = row
        if skill_filter and skill != skill_filter:
            continue
        lexical_score = seed_scores["lexical"]
        vector_seed = seed_scores["vector_seed"]
        learned_seed = seed_scores["learned_seed"]
        weights = json.loads(weights_json)
        direct_semantic = sparse_cosine(q_weights, q_norm, weights, norm)
        vector_semantic = sparse_cosine(q_vector_weights, q_vector_norm, json.loads(vector_weights_json), vector_norm) if q_vector_weights else 0.0
        topic_list = json.loads(topics_json)
        intent_list = json.loads(intents_json)
        use_for_list = json.loads(use_for_json)
        avoid_for_list = json.loads(avoid_for_json)
        tag_list = json.loads(tags_json)
        role_list = json.loads(roles_json)
        bundle_list = json.loads(bundles_json)
        query_context_tokens = set(query_token_set)
        query_context_tokens.update(tokenize(" ".join(inferred_intents)))
        query_context_tokens.update(tokenize(" ".join(expansion_terms)))
        tag_overlap = len(set(tag_list) & query_context_tokens)
        topic_overlap = len(set(topic_list) & query_context_tokens)
        intent_overlap = len(set(intent_list) & query_context_tokens)
        use_for_overlap = len(set(use_for_list) & query_context_tokens)
        avoid_overlap = len(set(avoid_for_list) & query_context_tokens)
        role_boost = 0.18 if path in role_paths else 0.0
        canonical_boost = 0.12 if is_canonical else 0.0
        tag_boost = min(tag_overlap * 0.05, 0.15)
        metadata_boost = min(topic_overlap * 0.08 + intent_overlap * 0.09 + use_for_overlap * 0.07, 0.32)
        avoid_penalty = min(avoid_overlap * 0.12, 0.24)
        confidence_boost = CONFIDENCE_BOOSTS.get(confidence, 0.0)
        path_prior_boost = max(min(learned_path_priors.get(path, 0.0) * 0.5, 0.22), -0.18)
        bundle_boost = 0.05 if role and any(role.lower() in bundle for bundle in bundle_list) else 0.0
        alias_boost = 0.08 if set(role_list) & query_token_set else 0.0
        feedback_boost = max(min(feedback_scores.get(path, 0.0) * 0.03, 0.12), -0.12)
        skill_feedback_boost = max(min(skill_feedback.get(skill, 0.0) * 0.02, 0.08), -0.08)
        section_boost = 0.0
        if section_kind in {"quick_read", "core_concept", "key_takeaways", "framework", "models_systems"}:
            section_boost = 0.04
        source_boost = 0.05 if source and source in query_token_set else 0.0
        intent_boost = 0.0
        if "ai_selection" in inferred_intents and source == "dejan":
            intent_boost += 0.08
        if "fan_out" in inferred_intents and source == "dejan":
            intent_boost += 0.06
        if "ai_visibility" in inferred_intents and source == "ahrefs":
            intent_boost += 0.05
        if "leak_systems" in inferred_intents and source == "hobo":
            intent_boost += 0.06
        type_adjust = 0.0
        if file_type == "memory_note":
            type_adjust = 0.08
        elif file_type == "reference_note":
            type_adjust = 0.06
        elif file_type in {"memory_router", "memory_index", "skill_contract", "squad_router"}:
            type_adjust = -0.08
        total = (
            lexical_score * 0.25
            + direct_semantic * 0.26
            + vector_semantic * 0.18
            + vector_seed * 0.12
            + max(min(learned_seed * 0.08, 0.12), 0.0)
            + role_boost
            + canonical_boost
            + tag_boost
            + metadata_boost
            + confidence_boost
            + path_prior_boost
            + bundle_boost
            + alias_boost
            + feedback_boost
            + skill_feedback_boost
            + section_boost
            + source_boost
            + intent_boost
            + freshness
            + type_adjust
            - avoid_penalty
        )
        ranked.append(
            {
                "chunk_id": chunk_id,
                "path": path,
                "skill": skill,
                "file_type": file_type,
                "heading": heading,
                "section_kind": section_kind,
                "source": source,
                "published_on": published_on,
                "inferred_intents": inferred_intents,
                "expansion_terms": expansion_terms,
                "score": round(total, 4),
                "lexical": round(lexical_score, 4),
                "semantic": round(direct_semantic, 4),
                "vector_semantic": round(vector_semantic, 4),
                "vector_seed": round(vector_seed, 4),
                "role_boost": role_boost,
                "canonical_boost": canonical_boost,
                "bundle_boost": bundle_boost,
                "alias_boost": alias_boost,
                "feedback_boost": feedback_boost,
                "skill_feedback_boost": skill_feedback_boost,
                "freshness_boost": freshness,
                "intent_boost": intent_boost,
                "metadata_boost": metadata_boost,
                "confidence_boost": confidence_boost,
                "path_prior_boost": path_prior_boost,
                "avoid_penalty": avoid_penalty,
                "confidence": confidence,
                "topics": topic_list,
                "intents": intent_list,
                "use_for": use_for_list,
                "roles": role_list,
                "bundles": bundle_list,
                "canonical_group": canonical_group,
                "snippet": text[:280].replace("\n", " "),
            }
        )

    con.close()
    return feedback_aware_top(ranked, top)


def decide(db_path: Path, query: str, role: Optional[str], top: int) -> dict:
    results = rank_chunks(db_path, query, role=role, skill_filter=None, top=max(top * 5, 20))
    expanded_query, inferred_intents, expansion_terms = expand_query(query, role)
    query_tokens = set(tokenize(expanded_query))
    squad_router = parse_squad_router(SKILLS_ROOT)
    role_key = role.lower() if role else None
    con = sqlite3.connect(str(db_path))
    try:
        learned_skill_priors = fetch_learned_skill_priors(con, inferred_intents)
    finally:
        con.close()
    skills = defaultdict(lambda: {"scores": [], "paths": set(), "headings": []})
    for item in results:
        if item["skill"] == "squad_router":
            continue
        if item["skill"] == "blank-agent-kit":
            continue
        if item["skill"].startswith(".system") and not (query_tokens & OPENAI_QUERY_HINTS):
            continue
        weight = item["score"]
        skills[item["skill"]]["scores"].append(weight)
        skills[item["skill"]]["paths"].add(item["path"])
        if len(skills[item["skill"]]["headings"]) < 3:
            skills[item["skill"]]["headings"].append(item["heading"])

    ranked_skills = sorted(
        (
            {
                "skill": skill,
                "score": round(
                    (
                        max(data["scores"])
                        + sum(sorted(data["scores"], reverse=True)[1:4]) * 0.35
                        + min(len(query_tokens & set(SKILL_ALIASES.get(skill, []))) * 0.45, 1.35)
                        + min(len(query_tokens & set(squad_router.skill_tags.get(skill, []))) * 0.35, 1.05)
                        + sum(INTENT_SKILL_PRIORS.get(intent, {}).get(skill, 0.0) for intent in inferred_intents)
                        + sum(
                            ROLE_INTENT_SKILL_PRIORS.get((role_key, intent), {}).get(skill, 0.0)
                            for intent in inferred_intents
                        )
                        + (0.0 if skill not in learned_skill_priors else max(min(learned_skill_priors[skill] * 0.85, 0.65), -0.35))
                    ),
                    4,
                ),
                "supporting_paths": sorted(data["paths"])[:5],
                "headings": data["headings"],
            }
            for skill, data in skills.items()
        ),
        key=lambda item: item["score"],
        reverse=True,
    )[:top]
    ranked_skills = apply_variant_preferences(ranked_skills, inferred_intents)

    preferred_skills = [item["skill"] for item in ranked_skills[:3]]
    primary_skill = preferred_skills[0] if preferred_skills else None
    allow_system_refs = bool(query_tokens & OPENAI_QUERY_HINTS)
    supporting_memory: List[dict] = []
    seen_chunk_ids = set()

    def collect(matches) -> None:
        for item in results:
            if item["chunk_id"] in seen_chunk_ids:
                continue
            if item["file_type"] == "squad_router":
                continue
            if item["skill"].startswith(".system") and not allow_system_refs:
                continue
            if not matches(item):
                continue
            supporting_memory.append(item)
            seen_chunk_ids.add(item["chunk_id"])
            if len(supporting_memory) >= top * 2:
                break

    collect(lambda item: item["skill"] == primary_skill and item["file_type"] in {"memory_note", "reference_note"})
    collect(lambda item: item["skill"] == primary_skill and item["file_type"] in {"skill_doc", "skill_contract", "memory_router"})
    collect(lambda item: item["skill"] in preferred_skills[1:] and item["file_type"] in {"memory_note", "reference_note"})
    collect(lambda item: item["skill"] in preferred_skills[1:] and item["file_type"] in {"skill_doc", "skill_contract", "memory_router"})
    collect(lambda item: item["file_type"] in {"memory_note", "reference_note"})
    collect(lambda item: item["file_type"] not in {"memory_index", "squad_router"})

    if len(supporting_memory) < top * 2:
        supporting_memory = results[: top * 2]

    return {
        "query": query,
        "role": role,
        "inferred_intents": inferred_intents,
        "expansion_terms": expansion_terms,
        "recommended_skills": ranked_skills,
        "supporting_memory": supporting_memory[: top * 2],
    }


def plan_for_pinchy(db_path: Path, query: str, top: int) -> dict:
    decision = decide(db_path, query, role="pinchy", top=top)
    skills = decision["recommended_skills"]
    primary = skills[0]["skill"] if skills else None
    support = [item["skill"] for item in skills[1:3]]
    memory = []
    seen_paths = set()
    for item in decision["supporting_memory"]:
        if item["path"] in seen_paths:
            continue
        seen_paths.add(item["path"])
        memory.append(item)
        if len(memory) >= 8:
            break
    themes = []
    for item in memory:
        theme = item["canonical_group"]
        if not theme and item.get("topics"):
            theme = item["topics"][0].replace("_", " ")
        if theme and theme not in themes:
            themes.append(theme)
    plan_steps = [
        "Identify the primary skill and supporting memory before answering.",
        "Use canonical notes first, then one supporting note per topic if needed.",
        "Keep the active context minimal and focused on the retrieved themes.",
    ]
    if memory:
        plan_steps.append("Prioritize the top-ranked memory notes before opening router or contract files.")

    return {
        "query": query,
        "role": "pinchy",
        "inferred_intents": decision.get("inferred_intents", []),
        "expansion_terms": decision.get("expansion_terms", []),
        "primary_skill": primary,
        "supporting_skills": support,
        "memory_themes": themes,
        "memory_shortlist": memory,
        "plan_steps": plan_steps,
    }


def build_task_pack_memory_shortlist(
    db_path: Path,
    query: str,
    role: Optional[str],
    pack: TaskPack,
    decision: dict,
    top: int,
    pack_path_priors: Optional[Dict[str, float]] = None,
) -> List[dict]:
    pack_path_priors = pack_path_priors or {}
    pool: List[dict] = []
    scoped_query = query
    if pack.memory_focus:
        scoped_query = f"{query} {' '.join(pack.memory_focus[:8])}"

    for skill in [pack.primary_skill] + pack.supporting_skills[:3]:
        if not skill:
            continue
        pool.extend(rank_chunks(db_path, scoped_query, role=role, skill_filter=skill, top=max(top * 2, 6)))
    pool.extend(decision.get("supporting_memory", []))

    ranked: List[dict] = []
    seen_chunk_ids = set()
    allow_system_refs = bool(set(tokenize(query)) & OPENAI_QUERY_HINTS)
    for item in pool:
        chunk_id = item.get("chunk_id")
        if chunk_id in seen_chunk_ids:
            continue
        seen_chunk_ids.add(chunk_id)
        if item["skill"].startswith(".system") and not allow_system_refs:
            continue
        focus_hits = pack_focus_overlap(item, pack)
        bonus = 0.0
        if item["skill"] == pack.primary_skill:
            bonus += 0.45
        elif item["skill"] in pack.supporting_skills:
            bonus += 0.25
        if item["file_type"] in {"memory_note", "reference_note"}:
            bonus += 0.12
        elif item["file_type"] in {"skill_doc", "skill_contract", "memory_router"}:
            bonus += 0.08
        bonus += min(focus_hits * 0.16, 0.48)
        bonus += max(min(pack_path_priors.get(item["path"], 0.0) * 0.5, 0.32), -0.2)
        if item.get("canonical_group"):
            bonus += 0.05
        ranked_item = dict(item)
        ranked_item["pack_score"] = round(item.get("score", 0.0) + bonus, 4)
        ranked_item["pack_focus_hits"] = focus_hits
        ranked.append(ranked_item)

    ranked.sort(key=lambda item: item["pack_score"], reverse=True)
    selected: List[dict] = []
    seen_paths = set()
    for item in ranked:
        if item["path"] in seen_paths:
            continue
        seen_paths.add(item["path"])
        selected.append(item)
        if len(selected) >= top:
            break
    return selected


def resolve_task_pack(
    db_path: Path,
    packs_path: Path,
    query: str,
    role: Optional[str],
    top: int,
    pack_id: Optional[str] = None,
) -> dict:
    decision = decide(db_path, query, role=role, top=max(top, 5))
    packs = load_task_packs(packs_path)
    con = sqlite3.connect(str(db_path))
    try:
        learned_pack_priors = fetch_learned_pack_priors(con, decision.get("inferred_intents", []), role)
    finally:
        con.close()

    scored_packs = [score_task_pack(pack, query, role, decision) for pack in packs]
    for item in scored_packs:
        learned_bonus = max(min(learned_pack_priors.get(item["pack"].pack_id, 0.0) * 0.85, 0.9), -0.5)
        if learned_bonus:
            item["score"] = round(item["score"] + learned_bonus, 4)
            item["reasons"].append(f"Learned outcome prior: {learned_bonus:+.2f}")
    scored_packs.sort(key=lambda item: item["score"], reverse=True)

    if pack_id:
        pack = find_task_pack(packs, pack_id)
        selected = next((item for item in scored_packs if item["pack"].pack_id == pack_id), None)
        if selected is None:
            selected = {"pack": pack, "score": 0.0, "reasons": []}
        selected = {
            **selected,
            "score": round(selected["score"] + 5.0, 4),
            "reasons": ["Explicit pack override", *selected["reasons"]],
        }
    else:
        if not scored_packs:
            raise ValueError(f"No task packs found in {packs_path}")
        selected = scored_packs[0]
        pack = selected["pack"]

    con = sqlite3.connect(str(db_path))
    try:
        pack_path_priors = fetch_pack_path_priors(con, pack.pack_id)
        pack_skill_priors = fetch_pack_skill_priors(con, pack.pack_id)
    finally:
        con.close()
    shortlist = build_task_pack_memory_shortlist(
        db_path,
        query,
        role,
        pack,
        decision,
        top=max(top * 2, 8),
        pack_path_priors=pack_path_priors,
    )
    themes = summarize_memory_themes(shortlist, limit=6)

    runner_ups = []
    for item in scored_packs:
        if item["pack"].pack_id == pack.pack_id:
            continue
        runner_ups.append(
            {
                "id": item["pack"].pack_id,
                "name": item["pack"].name,
                "score": item["score"],
            }
        )
        if len(runner_ups) >= 3:
            break

    return {
        "query": query,
        "role": role,
        "inferred_intents": decision.get("inferred_intents", []),
        "expansion_terms": decision.get("expansion_terms", []),
        "selected_pack": {
            **task_pack_to_dict(pack),
            "score": selected["score"],
            "reasons": selected["reasons"][:5],
        },
        "runner_up_packs": runner_ups,
        "recommended_skills": decision.get("recommended_skills", []),
        "learned_pack_skill_priors": pack_skill_priors,
        "memory_themes": themes,
        "memory_shortlist": shortlist,
    }


def build_execute_plan(
    db_path: Path,
    packs_path: Path,
    query: str,
    role: Optional[str],
    top: int,
    pack_id: Optional[str] = None,
) -> dict:
    task_pack = resolve_task_pack(db_path, packs_path, query, role, top, pack_id=pack_id)
    pack_info = task_pack["selected_pack"]
    recommended_skills = task_pack.get("recommended_skills", [])
    plan_steps = list(pack_info.get("checklist", []))
    if not plan_steps:
        plan_steps = [
            "Confirm the primary skill and scope.",
            "Load the top supporting memory before answering.",
            "Produce the deliverable in the required output format.",
        ]

    return {
        "query": query,
        "role": role,
        "inferred_intents": task_pack.get("inferred_intents", []),
        "expansion_terms": task_pack.get("expansion_terms", []),
        "selected_pack": pack_info,
        "recommended_skills": recommended_skills,
        "primary_skill": pack_info.get("primary_skill"),
        "supporting_skills": pack_info.get("supporting_skills", []),
        "memory_themes": task_pack.get("memory_themes", []),
        "memory_shortlist": task_pack.get("memory_shortlist", []),
        "execution_steps": plan_steps,
        "deliverables": pack_info.get("deliverables", []),
        "output_sections": pack_info.get("output_sections", []),
        "handoff_plan": pack_info.get("handoffs", []),
        "escalation_rules": pack_info.get("escalation_rules", []),
    }


def complete_task(
    db_path: Path,
    packs_path: Path,
    query: str,
    role: Optional[str],
    top: int,
    status: str,
    revision_count: int = 0,
    completion_minutes: Optional[float] = None,
    user_rating: Optional[float] = None,
    notes: str = "",
    pack_id: Optional[str] = None,
    used_paths: Optional[Sequence[str]] = None,
    used_skills: Optional[Sequence[str]] = None,
) -> dict:
    result = build_execute_plan(db_path, packs_path, query, role, top, pack_id=pack_id)
    primary_skill = result.get("primary_skill") or result["selected_pack"]["primary_skill"]
    supporting_skills = list(result.get("supporting_skills", []))
    memory_paths = [item["path"] for item in result.get("memory_shortlist", [])]

    resolved_used_paths = list(dict.fromkeys([path for path in (used_paths or memory_paths) if path]))
    resolved_used_skills = list(
        dict.fromkeys([skill for skill in (used_skills or ([primary_skill] + supporting_skills)) if skill])
    )

    con = sqlite3.connect(str(db_path))
    try:
        ensure_learning_tables(con)
        con.execute(
            """
            INSERT INTO task_outcomes(
              query, role, pack_id, primary_skill, supporting_skills_json, used_skills_json,
              memory_paths_json, status, revision_count, completion_minutes, user_rating, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                query,
                role,
                result["selected_pack"]["id"],
                primary_skill,
                json.dumps(supporting_skills),
                json.dumps(resolved_used_skills),
                json.dumps(resolved_used_paths),
                status,
                max(revision_count, 0),
                completion_minutes,
                user_rating,
                notes,
            ),
        )
        outcome_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        con.commit()
    finally:
        con.close()

    return {
        "db": str(db_path),
        "outcome_id": outcome_id,
        "query": query,
        "role": role,
        "status": status,
        "revision_count": max(revision_count, 0),
        "completion_minutes": completion_minutes,
        "user_rating": user_rating,
        "notes": notes,
        "selected_pack": result["selected_pack"],
        "primary_skill": primary_skill,
        "supporting_skills": supporting_skills,
        "used_skills": resolved_used_skills,
        "used_paths": resolved_used_paths,
    }


def train_pack_priors(db_path: Path) -> dict:
    con = sqlite3.connect(str(db_path))
    try:
        ensure_learning_tables(con)
        summary = train_pack_priors_in_connection(con)
        con.commit()
        return {"db": str(db_path), **summary}
    finally:
        con.close()


def pack_report(db_path: Path, packs_path: Path, limit: int) -> dict:
    packs = {pack.pack_id: pack for pack in load_task_packs(packs_path)}
    con = sqlite3.connect(str(db_path))
    try:
        ensure_learning_tables(con)
        task_outcome_count = con.execute("SELECT COUNT(*) FROM task_outcomes").fetchone()[0]
        last_training_row = con.execute(
            """
            SELECT ts, task_outcomes, pack_priors, pack_path_priors, pack_skill_priors
            FROM pack_training_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

        top_packs_rows = con.execute(
            """
            SELECT pack_id, score, accepted_count, revised_count, failed_count, exposure_count
            FROM learned_pack_priors
            WHERE bucket = ?
            ORDER BY score DESC, exposure_count DESC
            LIMIT ?
            """,
            (GLOBAL_BUCKET, limit),
        ).fetchall()
        weak_packs_rows = con.execute(
            """
            SELECT pack_id, score, accepted_count, revised_count, failed_count, exposure_count
            FROM learned_pack_priors
            WHERE bucket = ?
            ORDER BY score ASC, exposure_count DESC
            LIMIT ?
            """,
            (GLOBAL_BUCKET, limit),
        ).fetchall()
        top_paths_rows = con.execute(
            """
            SELECT pack_id, path, score, accepted_count, revised_count, failed_count, exposure_count
            FROM learned_pack_path_priors
            ORDER BY score DESC, exposure_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        weak_paths_rows = con.execute(
            """
            SELECT pack_id, path, score, accepted_count, revised_count, failed_count, exposure_count
            FROM learned_pack_path_priors
            ORDER BY score ASC, exposure_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        top_skills_rows = con.execute(
            """
            SELECT pack_id, skill, score, accepted_count, revised_count, failed_count, exposure_count
            FROM learned_pack_skill_priors
            ORDER BY score DESC, exposure_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        weak_skills_rows = con.execute(
            """
            SELECT pack_id, skill, score, accepted_count, revised_count, failed_count, exposure_count
            FROM learned_pack_skill_priors
            ORDER BY score ASC, exposure_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        high_revision_rows = con.execute(
            """
            SELECT pack_id,
                   COUNT(*) AS outcomes,
                   AVG(revision_count) AS avg_revision_count,
                   AVG(CASE WHEN user_rating IS NOT NULL THEN user_rating END) AS avg_user_rating
            FROM task_outcomes
            GROUP BY pack_id
            HAVING outcomes > 0
            ORDER BY avg_revision_count DESC, outcomes DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        con.close()

    def pack_name(pack_id: str) -> str:
        return packs.get(pack_id).name if pack_id in packs else pack_id

    def pack_dict(row) -> dict:
        pack_id, score, accepted_count, revised_count, failed_count, exposure_count = row
        return {
            "pack_id": pack_id,
            "pack_name": pack_name(pack_id),
            "score": score,
            "accepted_count": accepted_count,
            "revised_count": revised_count,
            "failed_count": failed_count,
            "exposure_count": exposure_count,
        }

    def path_dict(row) -> dict:
        pack_id, path, score, accepted_count, revised_count, failed_count, exposure_count = row
        return {
            "pack_id": pack_id,
            "pack_name": pack_name(pack_id),
            "path": path,
            "score": score,
            "accepted_count": accepted_count,
            "revised_count": revised_count,
            "failed_count": failed_count,
            "exposure_count": exposure_count,
        }

    def skill_dict(row) -> dict:
        pack_id, skill, score, accepted_count, revised_count, failed_count, exposure_count = row
        return {
            "pack_id": pack_id,
            "pack_name": pack_name(pack_id),
            "skill": skill,
            "score": score,
            "accepted_count": accepted_count,
            "revised_count": revised_count,
            "failed_count": failed_count,
            "exposure_count": exposure_count,
        }

    return {
        "db": str(db_path),
        "packs_file": str(packs_path),
        "task_outcomes": task_outcome_count,
        "last_pack_training": None
        if not last_training_row
        else {
            "ts": last_training_row[0],
            "task_outcomes": last_training_row[1],
            "pack_priors": last_training_row[2],
            "pack_path_priors": last_training_row[3],
            "pack_skill_priors": last_training_row[4],
        },
        "top_packs": [pack_dict(row) for row in top_packs_rows],
        "weak_packs": [pack_dict(row) for row in weak_packs_rows],
        "high_revision_packs": [
            {
                "pack_id": pack_id,
                "pack_name": pack_name(pack_id),
                "outcomes": outcomes,
                "avg_revision_count": round(avg_revision_count or 0.0, 3),
                "avg_user_rating": None if avg_user_rating is None else round(avg_user_rating, 3),
            }
            for pack_id, outcomes, avg_revision_count, avg_user_rating in high_revision_rows
        ],
        "top_pack_paths": [path_dict(row) for row in top_paths_rows],
        "weak_pack_paths": [path_dict(row) for row in weak_paths_rows],
        "top_pack_skills": [skill_dict(row) for row in top_skills_rows],
        "weak_pack_skills": [skill_dict(row) for row in weak_skills_rows],
    }


def log_query(db_path: Path, mode: str, query: str, role: Optional[str], skill_filter: Optional[str], top_n: int, result: dict) -> None:
    con = sqlite3.connect(str(db_path))
    con.execute(
        "INSERT INTO query_log(mode, query, role, skill_filter, top_n, result_json) VALUES (?, ?, ?, ?, ?, ?)",
        (mode, query, role, skill_filter, top_n, json.dumps(result)),
    )
    con.commit()
    con.close()


def add_feedback(db_path: Path, query: str, path: str, rating: str) -> None:
    con = sqlite3.connect(str(db_path))
    con.execute(
        "INSERT INTO feedback(query, path, rating) VALUES (?, ?, ?)",
        (query, path, rating),
    )
    con.commit()
    con.close()


def recent_logs(db_path: Path, limit: int) -> List[dict]:
    con = sqlite3.connect(str(db_path))
    rows = con.execute(
        "SELECT ts, mode, query, role, skill_filter, top_n FROM query_log ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    con.close()
    return [
        {
            "ts": ts,
            "mode": mode,
            "query": query,
            "role": role,
            "skill_filter": skill_filter,
            "top_n": top_n,
        }
        for ts, mode, query, role, skill_filter, top_n in rows
    ]


def query_role_lookup(con: sqlite3.Connection) -> Dict[str, Counter]:
    rows = con.execute("SELECT query, role FROM query_log WHERE role IS NOT NULL AND role != ''").fetchall()
    lookup: Dict[str, Counter] = defaultdict(Counter)
    for query, role in rows:
        lookup[query][role] += 1
    return lookup


def read_current_meta(rel_path: str) -> Dict[str, str]:
    path = SKILLS_ROOT / rel_path
    if not path.exists():
        return {}
    meta, _body = parse_frontmatter(path.read_text(encoding="utf-8"))
    return meta


def suggested_confidence(score: float, useful_count: int, not_useful_count: int) -> str:
    if useful_count >= 3 and score >= 0.45 and useful_count >= not_useful_count:
        return "high"
    if useful_count >= 1 and score >= 0.12:
        return "medium"
    return "low"


def confidence_rank(value: str) -> int:
    return {"": 0, "low": 1, "medium": 2, "high": 3}.get(value, 0)


def candidate_terms_for_queries(queries: Sequence[str]) -> List[str]:
    counts: Counter = Counter()
    for query in queries:
        _expanded, _intents, expansions = expand_query(query)
        if expansions:
            counts.update(
                term
                for term in expansions
                if term not in SUGGESTION_STOPWORDS and ("_" in term or len(term) >= 6)
            )
        else:
            counts.update(
                token
                for token in tokenize(query)
                if token not in SUGGESTION_STOPWORDS and len(token) > 2
            )
    return [term for term, _count in counts.most_common(8)]


def usage_report(db_path: Path, limit: int) -> dict:
    con = sqlite3.connect(str(db_path))
    try:
        ensure_learning_tables(con)
        query_log_count = con.execute("SELECT COUNT(*) FROM query_log").fetchone()[0]
        feedback_count = con.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
        last_training_row = con.execute(
            """
            SELECT ts, query_logs, feedback_rows, path_priors, skill_priors
            FROM training_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        top_paths = [
            {
                "path": path,
                "score": round(score, 4),
                "useful_count": useful_count,
                "not_useful_count": not_useful_count,
                "exposure_count": exposure_count,
            }
            for path, score, useful_count, not_useful_count, exposure_count in con.execute(
                """
                SELECT path, score, useful_count, not_useful_count, exposure_count
                FROM learned_path_priors
                WHERE bucket = ? AND score > 0
                ORDER BY score DESC, useful_count DESC
                LIMIT ?
                """,
                (GLOBAL_BUCKET, limit),
            ).fetchall()
        ]
        weak_paths = [
            {
                "path": path,
                "score": round(score, 4),
                "useful_count": useful_count,
                "not_useful_count": not_useful_count,
                "exposure_count": exposure_count,
            }
            for path, score, useful_count, not_useful_count, exposure_count in con.execute(
                """
                SELECT path, score, useful_count, not_useful_count, exposure_count
                FROM learned_path_priors
                WHERE bucket = ? AND exposure_count >= 2 AND (score < 0 OR not_useful_count > 0)
                ORDER BY score ASC, exposure_count DESC
                LIMIT ?
                """,
                (GLOBAL_BUCKET, limit),
            ).fetchall()
        ]
        top_skills = [
            {
                "skill": skill,
                "score": round(score, 4),
                "useful_count": useful_count,
                "not_useful_count": not_useful_count,
                "exposure_count": exposure_count,
            }
            for skill, score, useful_count, not_useful_count, exposure_count in con.execute(
                """
                SELECT skill, score, useful_count, not_useful_count, exposure_count
                FROM learned_skill_priors
                WHERE bucket = ? AND score > 0
                ORDER BY score DESC, useful_count DESC
                LIMIT ?
                """,
                (GLOBAL_BUCKET, limit),
            ).fetchall()
        ]
        bucket_rows = con.execute(
            """
            SELECT bucket, path, score
            FROM learned_path_priors
            WHERE bucket != ? AND score > 0
            ORDER BY bucket ASC, score DESC
            """,
            (GLOBAL_BUCKET,),
        ).fetchall()
        seen_buckets = set()
        top_buckets = []
        for bucket, path, score in bucket_rows:
            if bucket in seen_buckets:
                continue
            seen_buckets.add(bucket)
            top_buckets.append({"bucket": bucket, "top_path": path, "score": round(score, 4)})
            if len(top_buckets) >= limit:
                break
    finally:
        con.close()

    return {
        "db": str(db_path),
        "query_logs": query_log_count,
        "feedback_rows": feedback_count,
        "last_training": (
            None
            if not last_training_row
            else {
                "ts": last_training_row[0],
                "query_logs": last_training_row[1],
                "feedback_rows": last_training_row[2],
                "path_priors": last_training_row[3],
                "skill_priors": last_training_row[4],
            }
        ),
        "top_paths": top_paths,
        "weak_paths": weak_paths,
        "top_skills": top_skills,
        "top_buckets": top_buckets,
    }


def suggest_metadata(db_path: Path, limit: int, path_filter: Optional[str], min_useful: int) -> dict:
    con = sqlite3.connect(str(db_path))
    try:
        ensure_learning_tables(con)
        query_roles = query_role_lookup(con)
        if path_filter:
            candidate_rows = con.execute(
                """
                SELECT path, score, useful_count, not_useful_count, exposure_count
                FROM learned_path_priors
                WHERE bucket = ? AND path = ?
                """,
                (GLOBAL_BUCKET, path_filter),
            ).fetchall()
        else:
            candidate_rows = con.execute(
                """
                SELECT path, score, useful_count, not_useful_count, exposure_count
                FROM learned_path_priors
                WHERE bucket = ? AND useful_count >= ?
                ORDER BY score DESC, useful_count DESC
                LIMIT ?
                """,
                (GLOBAL_BUCKET, min_useful, limit),
            ).fetchall()

        suggestions = []
        for path, score, useful_count, not_useful_count, exposure_count in candidate_rows:
            meta = read_current_meta(path)
            current_intents = parse_meta_list(meta, "intent", "intents")
            current_roles = parse_meta_list(meta, "role", "roles")
            current_use_for = parse_meta_list(meta, "use_for")
            current_topics = parse_meta_list(meta, "topic", "topics")
            current_avoid = parse_meta_list(meta, "avoid_for")
            current_confidence = meta.get("confidence", "").strip().lower()
            current_canonical = parse_meta_bool(meta, "canonical")
            current_canonical_group = meta.get("canonical_group", "").strip()
            feedback_rows = con.execute(
                "SELECT query, rating FROM feedback WHERE path = ? ORDER BY id DESC",
                (path,),
            ).fetchall()
            useful_queries = [query for query, rating in feedback_rows if rating == "useful"]
            intent_counts: Counter = Counter()
            role_counts: Counter = Counter()
            for query in useful_queries:
                _expanded, inferred_intents, _expansions = expand_query(query)
                intent_counts.update(inferred_intents)
                role_counts.update(query_roles.get(query, Counter()))
            suggested_intents = [
                intent
                for intent, _count in intent_counts.most_common(4)
                if intent not in current_intents
            ]
            suggested_roles = [
                role
                for role, _count in role_counts.most_common(4)
                if role not in current_roles
            ]
            suggested_use_for = [
                term
                for term in candidate_terms_for_queries(useful_queries)
                if term not in (current_use_for + current_intents + current_topics)
            ][:5]
            if current_use_for:
                suggested_use_for = [term for term in suggested_use_for if "_" in term]
            topic_suggestion = current_topics[0] if current_topics else (suggested_intents[0] if suggested_intents else "")
            confidence_value = max(
                [current_confidence, suggested_confidence(float(score), useful_count, not_useful_count)],
                key=confidence_rank,
            )
            should_be_canonical = useful_count >= max(min_useful + 1, 3) and float(score) >= 0.35
            if not (
                (topic_suggestion and not current_topics)
                or suggested_intents
                or suggested_roles
                or suggested_use_for
                or confidence_rank(confidence_value) > confidence_rank(current_confidence)
                or (should_be_canonical and not current_canonical)
            ):
                continue
            suggestions.append(
                {
                    "path": path,
                    "training_signal": {
                        "score": round(float(score), 4),
                        "useful_count": useful_count,
                        "not_useful_count": not_useful_count,
                        "exposure_count": exposure_count,
                    },
                    "current_meta": {
                        "topic": meta.get("topic", ""),
                        "intent": current_intents,
                        "role": current_roles,
                        "use_for": current_use_for,
                        "avoid_for": current_avoid,
                        "confidence": current_confidence,
                        "canonical": current_canonical,
                        "canonical_group": current_canonical_group,
                    },
                    "suggested_meta": {
                        "topic": topic_suggestion,
                        "intent": suggested_intents,
                        "role": suggested_roles,
                        "use_for": suggested_use_for,
                        "confidence": confidence_value,
                        "canonical": current_canonical or should_be_canonical,
                        "canonical_group": current_canonical_group or topic_suggestion.replace("_", " "),
                    },
                }
            )
    finally:
        con.close()

    return {
        "db": str(db_path),
        "path_filter": path_filter,
        "min_useful": min_useful,
        "suggestions": suggestions,
    }


def evaluate_fixtures(db_path: Path, fixtures_path: Path) -> dict:
    fixtures = json.loads(fixtures_path.read_text(encoding="utf-8"))
    cases = fixtures["cases"]
    results = []
    primary_hits = 0
    skill_hits = 0
    path_hits = 0

    for case in cases:
        query = case["query"]
        role = case.get("role")
        expected_primary = case.get("expected_primary_skill")
        expected_skills = set(case.get("expected_skills", []))
        expected_paths = set(case.get("expected_paths", []))

        decision = decide(db_path, query, role=role, top=5)
        ranked_skills = [item["skill"] for item in decision["recommended_skills"]]
        memory_paths = [item["path"] for item in decision["supporting_memory"]]

        primary_ok = expected_primary == ranked_skills[0] if expected_primary and ranked_skills else False
        skill_ok = bool(expected_skills & set(ranked_skills[:3])) if expected_skills else True
        path_ok = bool(expected_paths & set(memory_paths[:5])) if expected_paths else True

        primary_hits += 1 if primary_ok else 0
        skill_hits += 1 if skill_ok else 0
        path_hits += 1 if path_ok else 0

        results.append(
            {
                "query": query,
                "role": role,
                "expected_primary_skill": expected_primary,
                "recommended_skills": ranked_skills[:5],
                "top_memory_paths": memory_paths[:5],
                "primary_hit": primary_ok,
                "skill_hit": skill_ok,
                "path_hit": path_ok,
                "inferred_intents": decision.get("inferred_intents", []),
            }
        )

    total = len(cases) or 1
    return {
        "fixture_path": str(fixtures_path),
        "total_cases": len(cases),
        "primary_skill_accuracy": round(primary_hits / total, 4),
        "top3_skill_hit_rate": round(skill_hits / total, 4),
        "top5_path_hit_rate": round(path_hits / total, 4),
        "results": results,
    }


def apply_variant_preferences(ranked_skills: List[dict], inferred_intents: List[str]) -> List[dict]:
    adjusted = list(ranked_skills)
    for intent in inferred_intents:
        for generic, specialist in INTENT_VARIANT_PREFERENCES.get(intent, []):
            generic_index = next((i for i, item in enumerate(adjusted) if item["skill"] == generic), None)
            specialist_index = next((i for i, item in enumerate(adjusted) if item["skill"] == specialist), None)
            if generic_index is None or specialist_index is None:
                continue
            if generic_index > specialist_index:
                continue
            generic_score = adjusted[generic_index]["score"]
            specialist_score = adjusted[specialist_index]["score"]
            if specialist_score + 1.0 >= generic_score:
                adjusted[generic_index], adjusted[specialist_index] = adjusted[specialist_index], adjusted[generic_index]
    return adjusted


def print_query_results(results: Sequence[dict]) -> None:
    for index, item in enumerate(results, start=1):
        print(f"{index}. [{item['score']:.4f}] {item['path']} :: {item['heading']}")
        print(
            f"   skill={item['skill']} type={item['file_type']} section={item['section_kind']} "
            f"source={item['source'] or '-'} lexical={item['lexical']} semantic={item['semantic']} vector={item['vector_semantic']}"
        )
        if item["canonical_group"]:
            print(f"   canonical={item['canonical_group']}")
        if item["topics"] or item["intents"] or item["confidence"]:
            print(
                f"   topics={', '.join(item['topics']) or '-'} "
                f"intents={', '.join(item['intents']) or '-'} "
                f"confidence={item['confidence'] or '-'}"
            )
        if item["published_on"]:
            print(f"   published_on={item['published_on']}")
        if item["bundles"]:
            print(f"   bundles={', '.join(item['bundles'])}")
        print(f"   {item['snippet']}")


def print_decision(result: dict) -> None:
    print(f"Query: {result['query']}")
    if result["role"]:
        print(f"Role: {result['role']}")
    print("\nRecommended skills:")
    for index, item in enumerate(result["recommended_skills"], start=1):
        print(f"{index}. {item['skill']} [{item['score']:.4f}]")
        print(f"   paths: {', '.join(item['supporting_paths'])}")
        print(f"   headings: {', '.join(item['headings'])}")
    print("\nSupporting memory:")
    print_query_results(result["supporting_memory"])


def print_pinchy_plan(result: dict) -> None:
    print(f"Query: {result['query']}")
    print(f"Role: {result['role']}")
    print(f"Primary skill: {result['primary_skill']}")
    print(f"Supporting skills: {', '.join(result['supporting_skills']) if result['supporting_skills'] else '(none)'}")
    if result["memory_themes"]:
        print(f"Memory themes: {', '.join(result['memory_themes'])}")
    print("\nPlan:")
    for idx, step in enumerate(result["plan_steps"], start=1):
        print(f"{idx}. {step}")
    print("\nMemory shortlist:")
    print_query_results(result["memory_shortlist"])


def print_task_pack(result: dict) -> None:
    pack = result["selected_pack"]
    print(f"Query: {result['query']}")
    if result["role"]:
        print(f"Role: {result['role']}")
    print(f"Selected pack: {pack['id']} ({pack['name']}) [{pack['score']:.4f}]")
    print(f"Primary skill: {pack['primary_skill']}")
    print(f"Supporting skills: {', '.join(pack['supporting_skills']) if pack['supporting_skills'] else '(none)'}")
    if pack["description"]:
        print(f"\nDescription: {pack['description']}")
    if pack["reasons"]:
        print("\nWhy this pack:")
        for reason in pack["reasons"]:
            print(f"- {reason}")
    if result["runner_up_packs"]:
        print("\nRunner-ups:")
        for item in result["runner_up_packs"]:
            print(f"- {item['id']} ({item['score']:.4f})")
    if result["memory_themes"]:
        print(f"\nMemory themes: {', '.join(result['memory_themes'])}")
    print("\nMemory shortlist:")
    print_query_results(result["memory_shortlist"])


def print_execute_plan(result: dict) -> None:
    pack = result["selected_pack"]
    print(f"Query: {result['query']}")
    if result["role"]:
        print(f"Role: {result['role']}")
    print(f"Execution pack: {pack['id']} ({pack['name']})")
    print(f"Primary skill: {result['primary_skill']}")
    print(f"Supporting skills: {', '.join(result['supporting_skills']) if result['supporting_skills'] else '(none)'}")
    if result["memory_themes"]:
        print(f"Memory themes: {', '.join(result['memory_themes'])}")
    print("\nExecution steps:")
    for idx, step in enumerate(result["execution_steps"], start=1):
        print(f"{idx}. {step}")
    if result["deliverables"]:
        print("\nDeliverables:")
        for item in result["deliverables"]:
            print(f"- {item}")
    if result["output_sections"]:
        print("\nOutput sections:")
        for item in result["output_sections"]:
            print(f"- {item}")
    if result["handoff_plan"]:
        print("\nHandoff plan:")
        for item in result["handoff_plan"]:
            print(f"- {item}")
    if result["escalation_rules"]:
        print("\nEscalation rules:")
        for item in result["escalation_rules"]:
            print(f"- {item}")
    print("\nMemory shortlist:")
    print_query_results(result["memory_shortlist"])


def print_completed_task(result: dict) -> None:
    print(f"Outcome ID: {result['outcome_id']}")
    print(f"Query: {result['query']}")
    if result["role"]:
        print(f"Role: {result['role']}")
    print(f"Status: {result['status']}")
    print(f"Pack: {result['selected_pack']['id']} ({result['selected_pack']['name']})")
    print(f"Primary skill: {result['primary_skill']}")
    print(f"Supporting skills: {', '.join(result['supporting_skills']) if result['supporting_skills'] else '(none)'}")
    print(f"Used skills: {', '.join(result['used_skills']) if result['used_skills'] else '(none)'}")
    if result["used_paths"]:
        print("Used paths:")
        for path in result["used_paths"]:
            print(f"- {path}")
    if result["completion_minutes"] is not None:
        print(f"Completion minutes: {result['completion_minutes']}")
    if result["user_rating"] is not None:
        print(f"User rating: {result['user_rating']}")
    if result["notes"]:
        print(f"Notes: {result['notes']}")


def print_train_summary(result: dict) -> None:
    print(f"DB: {result['db']}")
    if "query_logs" in result:
        print(f"Query logs: {result['query_logs']}")
    if "feedback_rows" in result:
        print(f"Feedback rows: {result['feedback_rows']}")
    if "path_priors" in result:
        print(f"Path priors: {result['path_priors']}")
    if "skill_priors" in result:
        print(f"Skill priors: {result['skill_priors']}")
    if "task_outcomes" in result:
        print(f"Task outcomes: {result['task_outcomes']}")
        print(f"Pack priors: {result['pack_priors']}")
        print(f"Pack path priors: {result['pack_path_priors']}")
        print(f"Pack skill priors: {result['pack_skill_priors']}")


def print_usage_report(result: dict) -> None:
    print(f"DB: {result['db']}")
    print(f"Query logs: {result['query_logs']}")
    print(f"Feedback rows: {result['feedback_rows']}")
    if result["last_training"]:
        print(
            "Last training: "
            f"{result['last_training']['ts']} "
            f"(logs={result['last_training']['query_logs']}, feedback={result['last_training']['feedback_rows']}, "
            f"path_priors={result['last_training']['path_priors']}, skill_priors={result['last_training']['skill_priors']})"
        )
    print("\nTop paths:")
    for item in result["top_paths"]:
        print(
            f"- {item['path']} score={item['score']:.4f} "
            f"useful={item['useful_count']} not_useful={item['not_useful_count']} exposure={item['exposure_count']}"
        )
    print("\nWeak paths:")
    for item in result["weak_paths"]:
        print(
            f"- {item['path']} score={item['score']:.4f} "
            f"useful={item['useful_count']} not_useful={item['not_useful_count']} exposure={item['exposure_count']}"
        )
    print("\nTop skills:")
    for item in result["top_skills"]:
        print(
            f"- {item['skill']} score={item['score']:.4f} "
            f"useful={item['useful_count']} not_useful={item['not_useful_count']} exposure={item['exposure_count']}"
        )


def print_pack_report(result: dict) -> None:
    print(f"DB: {result['db']}")
    print(f"Task outcomes: {result['task_outcomes']}")
    if result["last_pack_training"]:
        print(
            "Last pack training: "
            f"{result['last_pack_training']['ts']} "
            f"(outcomes={result['last_pack_training']['task_outcomes']}, "
            f"pack_priors={result['last_pack_training']['pack_priors']}, "
            f"pack_path_priors={result['last_pack_training']['pack_path_priors']}, "
            f"pack_skill_priors={result['last_pack_training']['pack_skill_priors']})"
        )
    print("\nTop packs:")
    for item in result["top_packs"]:
        print(
            f"- {item['pack_id']} ({item['pack_name']}) score={item['score']:.4f} "
            f"accepted={item['accepted_count']} revised={item['revised_count']} failed={item['failed_count']} exposure={item['exposure_count']}"
        )
    print("\nWeak packs:")
    for item in result["weak_packs"]:
        print(
            f"- {item['pack_id']} ({item['pack_name']}) score={item['score']:.4f} "
            f"accepted={item['accepted_count']} revised={item['revised_count']} failed={item['failed_count']} exposure={item['exposure_count']}"
        )
    print("\nHigh revision packs:")
    for item in result["high_revision_packs"]:
        rating = "-" if item["avg_user_rating"] is None else f"{item['avg_user_rating']:.2f}"
        print(
            f"- {item['pack_id']} ({item['pack_name']}) outcomes={item['outcomes']} "
            f"avg_revisions={item['avg_revision_count']:.2f} avg_rating={rating}"
        )
    print("\nTop pack paths:")
    for item in result["top_pack_paths"]:
        print(f"- {item['pack_id']} :: {item['path']} score={item['score']:.4f} exposure={item['exposure_count']}")
    print("\nWeak pack paths:")
    for item in result["weak_pack_paths"]:
        print(f"- {item['pack_id']} :: {item['path']} score={item['score']:.4f} exposure={item['exposure_count']}")
    print("\nTop pack skills:")
    for item in result["top_pack_skills"]:
        print(f"- {item['pack_id']} :: {item['skill']} score={item['score']:.4f} exposure={item['exposure_count']}")
    print("\nWeak pack skills:")
    for item in result["weak_pack_skills"]:
        print(f"- {item['pack_id']} :: {item['skill']} score={item['score']:.4f} exposure={item['exposure_count']}")


def print_metadata_suggestions(result: dict) -> None:
    for item in result["suggestions"]:
        print(item["path"])
        signal = item["training_signal"]
        print(
            f"  score={signal['score']:.4f} useful={signal['useful_count']} "
            f"not_useful={signal['not_useful_count']} exposure={signal['exposure_count']}"
        )
        print(f"  current={json.dumps(item['current_meta'], ensure_ascii=True)}")
        print(f"  suggested={json.dumps(item['suggested_meta'], ensure_ascii=True)}")


def print_eval(result: dict) -> None:
    print(f"Fixtures: {result['fixture_path']}")
    print(f"Total cases: {result['total_cases']}")
    print(f"Primary skill accuracy: {result['primary_skill_accuracy']:.2%}")
    print(f"Top-3 skill hit rate: {result['top3_skill_hit_rate']:.2%}")
    print(f"Top-5 path hit rate: {result['top5_path_hit_rate']:.2%}")
    print("\nCases:")
    for idx, item in enumerate(result["results"], start=1):
        print(f"{idx}. {item['query']}")
        print(f"   primary_hit={item['primary_hit']} skill_hit={item['skill_hit']} path_hit={item['path_hit']}")
        print(f"   skills={', '.join(item['recommended_skills'])}")


def main() -> None:
    args = parse_args()
    if args.command == "build":
        build_index(Path(args.root), Path(args.db))
        return
    if args.command == "query":
        results = rank_chunks(Path(args.db), args.text, args.role, args.skill, args.top)
        payload = {"query": args.text, "role": args.role, "skill_filter": args.skill, "results": results}
        log_query(Path(args.db), "query", args.text, args.role, args.skill, args.top, payload)
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print_query_results(results)
        return
    if args.command == "decide":
        result = decide(Path(args.db), args.text, args.role, args.top)
        log_query(Path(args.db), "decide", args.text, args.role, None, args.top, result)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_decision(result)
        return
    if args.command == "pinchy":
        result = plan_for_pinchy(Path(args.db), args.text, args.top)
        log_query(Path(args.db), "pinchy", args.text, "pinchy", None, args.top, result)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_pinchy_plan(result)
        return
    if args.command == "task-pack":
        result = resolve_task_pack(
            Path(args.db),
            Path(args.packs_file),
            args.text,
            args.role,
            args.top,
            pack_id=args.pack_id,
        )
        log_query(Path(args.db), "task-pack", args.text, args.role, None, args.top, result)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_task_pack(result)
        return
    if args.command == "execute-plan":
        result = build_execute_plan(
            Path(args.db),
            Path(args.packs_file),
            args.text,
            args.role,
            args.top,
            pack_id=args.pack_id,
        )
        log_query(Path(args.db), "execute-plan", args.text, args.role, None, args.top, result)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_execute_plan(result)
        return
    if args.command == "complete-task":
        result = complete_task(
            Path(args.db),
            Path(args.packs_file),
            args.text,
            args.role,
            args.top,
            args.status,
            revision_count=args.revision_count,
            completion_minutes=args.completion_minutes,
            user_rating=args.user_rating,
            notes=args.notes,
            pack_id=args.pack_id,
            used_paths=args.used_paths,
            used_skills=args.used_skills,
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_completed_task(result)
        return
    if args.command == "feedback":
        add_feedback(Path(args.db), args.query, args.path, args.rating)
        print(f"Recorded {args.rating} feedback for {args.path}")
        return
    if args.command == "logs":
        rows = recent_logs(Path(args.db), args.limit)
        print(json.dumps(rows, indent=2))
        return
    if args.command == "train":
        result = train_usage_priors(Path(args.db))
        payload = {"db": str(Path(args.db)), **result}
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print_train_summary(payload)
        return
    if args.command == "report":
        result = usage_report(Path(args.db), args.limit)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_usage_report(result)
        return
    if args.command == "pack-train":
        result = train_pack_priors(Path(args.db))
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_train_summary(result)
        return
    if args.command == "pack-report":
        result = pack_report(Path(args.db), Path(args.packs_file), args.limit)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_pack_report(result)
        return
    if args.command == "suggest-metadata":
        result = suggest_metadata(Path(args.db), args.limit, args.path, args.min_useful)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_metadata_suggestions(result)
        return
    if args.command == "eval":
        result = evaluate_fixtures(Path(args.db), Path(args.fixtures))
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print_eval(result)
        return


if __name__ == "__main__":
    main()
