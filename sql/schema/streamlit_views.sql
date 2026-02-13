-- New queries

--1. Check how many replies in a douban post are not labeled by LLM.
CREATE OR REPLACE VIEW v_reply_amount_unlabeled AS
SELECT COUNT(*) AS remaining
FROM public.douban_topic_post_raw r
WHERE  
  r.content_text IS NOT NULL AND 
  length(btrim(r.content_text)) > 0 AND 
  NOT EXISTS (
    SELECT 1
    FROM public.douban_topic_post_ai a
    WHERE a.topic_id = r.topic_id
      AND a.post_type = r.post_type
      AND a.floor_no  = r.floor_no
      AND a.prompt_version = 'v1_qwen3_4b_json'
  );
  
  
  
--2. Check the comment analysis to see which haters are the most active 
-- (excluding Randy's fans who were misjudged).
-- DROP VIEW v_active_haters
CREATE OR REPLACE VIEW v_active_haters AS
WITH base AS (
  SELECT
    r.user_id,
    r.user_name,
    r.topic_id,
    r.topic_title,
    r.post_type,
    r.floor_no,
    r.content_text,
    r.pubtime,
    a.ai_label,
	a.labeled_at,
    COUNT(*) OVER (PARTITION BY r.user_id) AS reply_count
  FROM douban_topic_post_ai a
  JOIN douban_topic_post_raw r
    ON a.topic_id IS NOT DISTINCT FROM r.topic_id
   AND a.post_type IS NOT DISTINCT FROM r.post_type
   AND a.floor_no  IS NOT DISTINCT FROM r.floor_no
  WHERE a.ai_label = 'hater'
	AND r.user_id NOT IN(
	SELECT member_id
	FROM douban_group_members
	WHERE group_id = 754923		 --萌物组粉丝
	)
)
SELECT
  user_name,
  reply_count,
  RANK() OVER (ORDER BY reply_count DESC) AS rnk,
  topic_title,
  post_type,
  floor_no,
  content_text,
  ai_label,
  pubtime,
  user_id,
  labeled_at
FROM base
--ORDER BY reply_count DESC, user_id, pubtime;
  
  
--3. How many other group members appear in 754923
-- DROP VIEW v_members_distribution
CREATE OR REPLACE VIEW v_members_distribution AS
WITH base AS (
  SELECT member_id
  FROM douban_group_members
  WHERE group_id = 754923         -- 萌物
)
SELECT
  m.group_id AS group_id,
  g.group_name AS group_name,
  g.group_who AS group_who,
  COUNT(m.member_id) AS member_cnt
FROM douban_group_members m
JOIN base b
  ON b.member_id = m.member_id
JOIN douban_groups g
  ON m.group_id = g.group_id
WHERE m.group_id <> 754923
GROUP BY m.group_id,g.group_name,g.group_who
--ORDER BY member_cnt DESC, g.group_who;



--4. Distribution of active users in groups
CREATE OR REPLACE VIEW v_reply_users_distribution AS
SELECT
  r.user_id,
  r.user_name,
  COUNT(*) AS reply_count,
  RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk,
  array_agg(DISTINCT g.group_name ORDER BY g.group_name) AS group_names
FROM douban_topic_post_ai a
JOIN douban_topic_post_raw r
  ON a.topic_id IS NOT DISTINCT FROM r.topic_id
 AND a.post_type IS NOT DISTINCT FROM r.post_type
 AND a.floor_no  IS NOT DISTINCT FROM r.floor_no
LEFT JOIN douban_group_members m
  ON r.user_id = m.member_id
LEFT JOIN douban_groups g
  ON m.group_id = g.group_id
  
--WHERE a.ai_label = 'hater'                -- 标注为黑子的
  --AND r.user_id NOT IN (
  --      SELECT member_id
  --      FROM douban_group_members
  --      WHERE group_id IN (742550,754719)  -- 除去有趣读书组,仙女教母
  --    )
GROUP BY
  r.user_id,
  r.user_name
--ORDER BY reply_count DESC;


--5. 1-star low rating members distribution
-- DROP VIEW v_lowrating_users_distribution
CREATE OR REPLACE VIEW v_lowrating_users_distribution AS
SELECT
  dgm.group_id AS group_id,
  dg.group_name AS group_name,
  dg.group_who AS group_who,
  COUNT(DISTINCT dgm.member_id) AS user_cnt
FROM (
  SELECT DISTINCT user_id
  FROM low_rating_users
  WHERE drama_id = '36317401'  --朝雪录
) lru
JOIN douban_group_members dgm
  ON dgm.member_id = lru.user_id
JOIN douban_groups dg
  ON dg.group_id = dgm.group_id
GROUP BY dgm.group_id, dg.group_name,dg.group_who
--ORDER BY user_cnt DESC;
 