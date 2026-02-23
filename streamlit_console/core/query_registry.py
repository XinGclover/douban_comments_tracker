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
    track_id_keys: list[str] | None = None

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
          array_agg(g.group_name ORDER BY g.group_name) AS group_names,
          array_agg(DISTINCT g.group_who ORDER BY g.group_who) AS group_whos
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
        track_id_keys=["member_id"],
    ),
    QueryDef(
        name="🤡 其他组组员在萌物组（按 group_who）",
        category="Douban Members",
        desc="查看某个 group_who 的成员中，有哪些人在萌物组（group_id=754923）里",
        sql="""
        WITH who_groups AS (
            SELECT group_id
            FROM douban_groups
            WHERE group_who = %(group_who)s
            ),
            overlap_members AS (
            SELECT m.member_id
            FROM douban_group_members m
            WHERE m.group_id IN (SELECT group_id FROM who_groups)
                AND EXISTS (
                SELECT 1
                FROM douban_group_members x
                WHERE x.member_id = m.member_id
                    AND x.group_id = 754923
                )
            GROUP BY m.member_id
            )
            SELECT
            m.member_id,
            MAX(m.member_name) AS member_name,
            array_agg(DISTINCT g.group_name ORDER BY g.group_name) AS all_groups
            FROM douban_group_members m
            JOIN overlap_members o
            ON o.member_id = m.member_id
            JOIN douban_groups g
            ON g.group_id = m.group_id
            GROUP BY m.member_id
            ORDER BY m.member_id;
        """,
        params=[
            QueryParam(
                key="group_who",
                label="group_who",
                type="text",
                required=True,
                placeholder="兰迪",
                help="Douban group_who 字段，某明星的组",
            ),
        ],
        default_limit=500,
    ),
    QueryDef(
        name="📊 查某用户对兰迪发表过哪些评论（按 user_id）",
        category="Douban Posts",
        desc="查看某用户对兰迪发表过哪些评论",
        sql="""
        SELECT
            p.user_name,
            p.content_text,
            p.topic_id,
            p.topic_title,
            p.post_type,
            p.floor_no,
            p.pubtime,
            t.group_name,
            p.ip_location,
            p.like_count
        FROM douban_topic_post_raw p
        JOIN other_group_topics t
        ON p.topic_id = t.topic_id
        WHERE user_id = %(user_id)s
        """,
        params=[
            QueryParam(
                key="user_id",
                label="user_id",
                type="text",
                required=True,
                placeholder="222984488",
                help="Douban user ID",
            ),
        ],
        default_limit=500,
        track_id_keys=["user_id"]
    ),
    QueryDef(
        name="📊 萌物组卧底发表过的对兰迪的评论（按 group_who）",
        category="Douban Posts",
        desc="查看某 group_who 组员在萌物组（group_id=754923）发表过的对兰迪的评论",
        sql="""
        WITH who_groups AS (
            SELECT group_id
            FROM douban_groups
            WHERE group_who = %(group_who)s
            ),
            overlap_members AS (
            SELECT DISTINCT m.member_id
            FROM douban_group_members m
            WHERE m.group_id IN (SELECT group_id FROM who_groups)
                AND EXISTS (
                SELECT 1
                FROM douban_group_members x
                WHERE x.member_id = m.member_id
                    AND x.group_id = 754923
                )
            )
        SELECT
        p.user_id,
        p.user_name,
        p.content_text,
        p.topic_id,
        p.topic_title,
        p.post_type,
        p.floor_no,
        p.pubtime,
        t.group_name,
        p.ip_location,
        p.like_count
        FROM douban_topic_post_raw p
        JOIN other_group_topics t
        ON p.topic_id = t.topic_id
        JOIN overlap_members o
        ON p.user_id = o.member_id
        ORDER BY p.pubtime DESC;
        """,
        params=[
            QueryParam(
                key="group_who",
                label="group_who",
                type="text",
                required=True,
                placeholder="兰迪",
                help="Douban group_who 字段，某明星的组",
            ),
        ],
        default_limit=1000,
    ),
    QueryDef(
        name="📊 其他组组员发表过的对兰迪的评论（按 group_who）",
        category="Douban Posts",
        desc="查看某 group_who 组员发表过的对兰迪的评论",
        sql="""
        WITH who_groups AS (
            SELECT group_id
            FROM douban_groups
            WHERE group_who = %(group_who)s
            ),
            overlap_members AS (
            SELECT DISTINCT m.member_id
            FROM douban_group_members m
            WHERE m.group_id IN (SELECT group_id FROM who_groups)
                AND EXISTS (
                SELECT 1
                FROM douban_group_members x
                WHERE x.member_id = m.member_id
                )
            )
        SELECT
        p.user_id,
        p.user_name,
        p.content_text,
        p.topic_id,
        p.topic_title,
        p.post_type,
        p.floor_no,
        p.pubtime,
        t.group_name,
        p.ip_location,
        p.like_count
        FROM douban_topic_post_raw p
        JOIN other_group_topics t
        ON p.topic_id = t.topic_id
        JOIN overlap_members o
        ON p.user_id = o.member_id
        ORDER BY p.pubtime DESC;
        """,
        params=[
            QueryParam(
                key="group_who",
                label="group_who",
                type="text",
                required=True,
                placeholder="兰迪",
                help="Douban group_who 字段，某明星的组",
            ),
        ],
        default_limit=1000,
    ),
    QueryDef(
        name="📊 查询某明星后花园粉丝发帖情况（按 group_who）",
        category="Douban Topics",
        desc="按 topic 汇总版：每个 topic 一行 + OP作者 + 该 OP 作者属于哪些 member groups（同一个 group_who 内）",
        sql="""
        WITH who_groups AS (
        SELECT group_id, group_name
        FROM douban_groups
        WHERE group_who = %(group_who)s
        ),
        op_rows AS (
        SELECT
            r.topic_id,
            r.topic_title,
            r.topic_url,
            COALESCE(r.pubtime, t.created_at) AS op_pubtime,
            r.user_id AS op_user_id,
            COALESCE(NULLIF(btrim(r.user_name), ''), m.member_name) AS op_user_name,
            t.group_id   AS topic_group_id,
            t.group_name AS topic_group_name,
            wg.group_id  AS member_group_id,
            wg.group_name AS member_group_name
        FROM douban_topic_post_raw r
        JOIN douban_group_members m
            ON m.member_id = r.user_id
        JOIN who_groups wg
            ON wg.group_id = m.group_id
        LEFT JOIN other_group_topics t
            ON t.topic_id = r.topic_id
        WHERE r.post_type = 'op'
        )
        SELECT
        topic_id,
        topic_title,
        op_pubtime,
        op_user_id,
        op_user_name,
        topic_group_name,
        array_agg(DISTINCT member_group_id ORDER BY member_group_id) AS member_group_ids,
        array_agg(DISTINCT member_group_name ORDER BY member_group_name) AS member_group_names,
        topic_url
        FROM op_rows
        GROUP BY
        topic_id, topic_title, topic_url, op_pubtime, op_user_id, op_user_name,
        topic_group_id, topic_group_name
        ORDER BY op_pubtime DESC NULLS LAST;
        """,
        params=[
            QueryParam(
                key="group_who",
                label="group_who",
                type="text",
                required=True,
                placeholder="兰迪",
                help="Douban group_who 字段，某明星的组",
            ),
        ],
    )
]