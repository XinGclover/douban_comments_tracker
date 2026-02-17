-- Demo Views

--1. Distribution of active users in groups
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
  
WHERE a.ai_label = 'hater'              
  AND r.user_id NOT IN (
        SELECT member_id
        FROM douban_group_members
        WHERE group_id IN (742550,754719)  -- 除去有趣读书组,仙女教母
      )
GROUP BY
  r.user_id,
  r.user_name
ORDER BY reply_count DESC;

-- Materialize the view into a table
CREATE TABLE demo_reply_users_distribution AS
SELECT * FROM v_reply_users_distribution;

--------------------------------------------------------------------
--2. 1-star low rating members distribution
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


-- Materialize the view into a table
CREATE TABLE demo_lowrating_users_distribution AS
SELECT * FROM v_lowrating_users_distribution;
--------------------------------------------------------------------


--3.Create a view of high rating dramas by low rating users for zhaoxuelu
--DROP VIEW IF EXISTS view_high_rating_dramas_source_zhaoxuelu;
CREATE OR REPLACE VIEW view_high_rating_dramas_source_zhaoxuelu AS
SELECT
    high_rating_drama_id,
    drama_name,
    high_rating_user_count
FROM view_high_rating_dramas_source
WHERE source_drama_id = '36317401'    
ORDER BY high_rating_user_count DESC;

-- Materialize the view into a table
CREATE TABLE demo_high_rating_dramas_source_zhaoxuelu AS
SELECT * FROM view_high_rating_dramas_source_zhaoxuelu;