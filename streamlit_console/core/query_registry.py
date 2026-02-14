from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Literal

ParamType = Literal["text", "int", "float", "date", "datetime", "select"]

@dataclass(frozen=True)
class QueryParam:
    key: str
    label: str
    type: ParamType = "text"
    required: bool = True
    default: Any = None
    placeholder: str = ""
    help: str = ""
    options: Optional[List[str]] = None  # for select


@dataclass(frozen=True)
class QueryDef:
    name: str
    category: str
    desc: str
    sql: str
    params: List[QueryParam]
    # optional: default limit for this query (overridable by UI input)
    default_limit: Optional[int] = 500


CATEGORIES = [
    "Douban Members",
    "Douban Topics",
    "Douban Posts",
    "Douban Comments",
    "LLM Analysis",
]

QUERIES: List[QueryDef] = [
    QueryDef(
        name="👥 用户在哪些组（按 member_id）",
        category="Douban Members",
        desc="输入 member_id，查看该用户出现过的 group_name 列表（聚合）",
        sql="""
        SELECT
          m.member_id,
          m.member_name,
          array_agg(g.group_name ORDER BY g.group_name) AS group_names
        FROM douban_group_members m
        JOIN douban_groups g
          ON m.group_id = g.group_id
        WHERE m.member_id = %(member_id)s
        GROUP BY m.member_id, m.member_name
        """,
        params=[
            QueryParam(
                key="member_id",
                label="member_id",
                type="text",
                required=True,
                placeholder="239300232",
                help="Douban user ID",
            ),
        ],
        default_limit=200,
    )
]
