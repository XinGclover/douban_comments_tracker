-- Create a view of how iqiyi heat changes over time
--DROP VIEW IF EXISTS view_zhaoxuelu_heat_iqiyi_with_shanghai_time;

CREATE OR REPLACE VIEW view_zhaoxuelu_heat_iqiyi_with_shanghai_time AS
SELECT 
	insert_time,
    insert_time AT TIME ZONE 'Asia/Shanghai' AS insert_time_shanghai,
    heat_info
FROM public.zhaoxuelu_heat_iqiyi
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00'
ORDER by insert_time DESC;
	
	
-- Create a view of how douban total_comments，total_reviews，total_discussions changes over time
--DROP VIEW IF EXISTS view_zhaoxuelu_comments_count_with_shanghai_time;

CREATE OR REPLACE VIEW view_zhaoxuelu_comments_count_with_shanghai_time AS
SELECT 
    insert_time AT TIME ZONE 'Asia/Shanghai' AS insert_time_shanghai,
	total_comments,
	total_reviews,
	total_discussions
FROM public.zhaoxuelu_comments_count
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00'
ORDER by insert_time DESC;


-- Create a view of all ratings percentage
--DROP VIEW IF EXISTS view_zhaoxuelu_comments_rating_percentage;

CREATE OR REPLACE VIEW view_zhaoxuelu_comments_rating_percentage AS
SELECT 
   rating, 
   count(*) as rating_count, 
   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS rating_percent
FROM public.zhaoxuelu_comments
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00'
GROUP by rating
ORDER by rating DESC;

-- Create a view of all ratings percentage changes with time
--DROP VIEW IF EXISTS view_zhaoxuelu_comments_rating_percentage_with_time;

CREATE OR REPLACE VIEW view_zhaoxuelu_comments_rating_percentage_with_time AS
SELECT 
   insert_time,
   rating, 
   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS rating_percent
FROM public.zhaoxuelu_comments
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00'
GROUP by insert_time, rating
ORDER by rating DESC;

-- Create a view of all ratings percentage changes dayily
CREATE OR REPLACE VIEW view_zhaoxuelu_comments_rating_percentage_daily AS
SELECT 
   DATE(insert_time) AS comment_date,
   rating, 
   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY DATE(insert_time)), 2) AS rating_percent
FROM public.zhaoxuelu_comments
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00'
GROUP BY DATE(insert_time), rating
ORDER BY comment_date, rating DESC;



-- Create a view of distribution of different ratings in different regions
--DROP VIEW IF EXISTS view_zhaoxuelu_comments_distribution;

CREATE OR REPLACE VIEW view_zhaoxuelu_comments_distribution AS
SELECT 
    user_location,
    COUNT(*) FILTER (WHERE rating = 1) AS rating_1_count,
    COUNT(*) FILTER (WHERE rating = 2) AS rating_2_count,
    COUNT(*) FILTER (WHERE rating = 3) AS rating_3_count,
    COUNT(*) FILTER (WHERE rating = 4) AS rating_4_count,
    COUNT(*) FILTER (WHERE rating = 5) AS rating_5_count,
	COUNT(*) FILTER (WHERE rating IS NULL) AS no_rating_count,
    COUNT(*) AS total_count
FROM public.zhaoxuelu_comments
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00'
GROUP by user_location
ORDER by total_count DESC;


--Create a long view of distribution of different ratings in different regions
CREATE OR REPLACE VIEW view_zhaoxuelu_comments_distribution_long AS
SELECT user_location, 'rating_1' AS rating_category, rating_1_count AS count FROM view_zhaoxuelu_comments_distribution
UNION ALL
SELECT user_location, 'rating_2' AS rating_category, rating_2_count AS count FROM view_zhaoxuelu_comments_distribution
UNION ALL
SELECT user_location, 'rating_3' AS rating_category, rating_3_count AS count FROM view_zhaoxuelu_comments_distribution
UNION ALL
SELECT user_location, 'rating_4' AS rating_category, rating_4_count AS count FROM view_zhaoxuelu_comments_distribution
UNION ALL
SELECT user_location, 'rating_5' AS rating_category, rating_5_count AS count FROM view_zhaoxuelu_comments_distribution
UNION ALL
SELECT user_location, 'no_rating' AS rating_category, no_rating_count AS count FROM view_zhaoxuelu_comments_distribution;


--Create a long view of comparison of timeline of iqiyi heat
--DROP VIEW IF EXISTS view_zhaoxuelu_iqiyiheat_timeline;

CREATE OR REPLACE VIEW view_zhaoxuelu_iqiyiheat_timeline AS
SELECT 
    to_char(timezone('Asia/Shanghai', insert_time), 'HH24:MI:SS') AS time_part,
    date(timezone('Asia/Shanghai', insert_time)) AS date_part,
    heat_info
FROM zhaoxuelu_heat_iqiyi
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00';

--DROP VIEW IF EXISTS view_zhaoxuelu_iqiyiheat_timeline;


--Create a view of high rating dramas by low rating users for one source drama
--DROP VIEW IF EXISTS view_high_rating_dramas_source; 
CREATE OR REPLACE VIEW view_high_rating_dramas_source AS
SELECT
    l.drama_id AS source_drama_id,
    c.drama_id AS high_rating_drama_id,
    d.drama_name,
    COUNT(DISTINCT c.user_id) AS high_rating_user_count
FROM low_rating_users l
JOIN drama_collection c ON l.user_id = c.user_id
JOIN douban_drama_info d ON c.drama_id = d.drama_id
WHERE c.rating = 5
  AND c.drama_id <> l.drama_id  
GROUP BY l.drama_id, c.drama_id, d.drama_name
ORDER BY l.drama_id, high_rating_user_count DESC;

--Create a view of high rating dramas by low rating users for one source drama with time
--DROP VIEW IF EXISTS view_high_rating_dramas_source_with_time; 
CREATE OR REPLACE VIEW view_high_rating_dramas_source_with_time AS
SELECT
    l.user_id,
    l.drama_id AS source_drama_id,
    c.drama_id AS high_rating_drama_id,
    d.drama_name,
    COUNT(DISTINCT c.user_id) AS high_rating_user_count,
	l.comment_time
FROM low_rating_users l
JOIN drama_collection c ON l.user_id = c.user_id
JOIN douban_drama_info d ON c.drama_id = d.drama_id
WHERE c.rating = 5
  AND c.drama_id <> l.drama_id  
GROUP BY l.drama_id, c.drama_id, d.drama_name,l.comment_time, l.user_id
ORDER BY l.drama_id, high_rating_user_count DESC;


--Create a view of high rating dramas by low rating users for zhaoxuelu
--DROP VIEW IF EXISTS view_high_rating_dramas_source_zhaoxuelu;
CREATE OR REPLACE VIEW view_high_rating_dramas_source_zhaoxuelu AS
SELECT
    high_rating_drama_id,
    drama_name,
    high_rating_user_count
FROM view_high_rating_dramas_source
WHERE source_drama_id = '36317401'    
ORDER BY high_rating_user_count DESC;


--Create a view of high rating dramas by low rating users for zhaoxuelu changes with date
--DROP VIEW IF EXISTS view_high_rating_dramas_source_zhaoxuelu_with_time;
CREATE OR REPLACE VIEW view_high_rating_dramas_source_zhaoxuelu_with_time AS
SELECT
    high_rating_drama_id,
    drama_name,
    COUNT(DISTINCT user_id) AS high_rating_user_count,
    DATE(comment_time) AS comment_day    
FROM view_high_rating_dramas_source_with_time
WHERE source_drama_id = '36317401'
GROUP BY high_rating_drama_id, drama_name, comment_day
ORDER BY comment_day, high_rating_user_count DESC;


--Create a view of Top 10 high rating dramas by low rating users for zhaoxuelu changes with date
--DROP VIEW IF EXISTS view_high_rating_dramas_daily_top;
CREATE OR REPLACE VIEW view_high_rating_dramas_daily_top AS
WITH ranked_dramas AS (
    SELECT
        high_rating_drama_id,
        drama_name,
        high_rating_user_count,
        comment_day,
        ROW_NUMBER() OVER (PARTITION BY comment_day ORDER BY high_rating_user_count DESC) AS rank
    FROM view_high_rating_dramas_source_zhaoxuelu_with_time
)
SELECT *
FROM ranked_dramas
WHERE rank <= 10;


--DROP VIEW IF EXISTS view_cumulative_top20_dramas_per_day;
CREATE OR REPLACE VIEW view_cumulative_top20_dramas_per_day AS
WITH cumulative_counts AS (
    SELECT
        comment_day,
        drama_name,
        SUM(high_rating_user_count) OVER (
            PARTITION BY drama_name
            ORDER BY comment_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_count
    FROM view_high_rating_dramas_source_zhaoxuelu_with_time
),
ranked AS (
    SELECT
        comment_day,
        drama_name,
        cumulative_count,
        ROW_NUMBER() OVER (PARTITION BY comment_day ORDER BY cumulative_count DESC) AS rank
    FROM cumulative_counts
)
SELECT *
FROM ranked
WHERE rank <= 20;




--Create a view of all high frequency words
--DROP VIEW IF EXISTS view_zhaoxuelu_top_words;

CREATE OR REPLACE VIEW view_zhaoxuelu_top_words AS
SELECT 
    word,
    COUNT(*) AS freq
FROM zhaoxuelu_comment_words
WHERE create_time >= TIMESTAMP '2025-07-13 04:00:00'
GROUP BY word
ORDER BY freq DESC
LIMIT 100;


--Create a view of daily word frequency trend
--DROP VIEW IF EXISTS view_zhaoxuelu_daily_top_words;

CREATE OR REPLACE VIEW view_zhaoxuelu_daily_top_words AS
SELECT 
    DATE_TRUNC('day', create_time) AS day,
    word,
    COUNT(*) AS freq
FROM zhaoxuelu_comment_words
WHERE create_time >= CURRENT_DATE
  	AND create_time < CURRENT_DATE + INTERVAL '1 day'
GROUP BY day, word
ORDER BY day, freq DESC
LIMIT 100;



--Create a view of top words of rating=1
CREATE OR REPLACE VIEW view_zhaoxuelu_top_words_rating1 AS
SELECT 
    word,
    COUNT(*) AS freq
FROM zhaoxuelu_comment_words
WHERE create_time >= TIMESTAMP '2025-07-13 04:00:00' 
GROUP BY word
ORDER BY freq DESC
LIMIT 100;


--Create a view of trend of weibo users
--DROP VIEW IF EXISTS view_weibo_stats;
CREATE OR REPLACE VIEW view_weibo_stats AS
SELECT
    u.user_name,
    s.followers_count,
    s.likes_count,
	s.recorded_at
FROM weibo_user_stats s
JOIN weibo_user u ON s.user_id = u.user_id
WHERE recorded_at >= TIMESTAMP '2025-07-13 04:00:00';




--Create a view of increment of weibo users, the period is the same as view_weibo_stats
CREATE OR REPLACE VIEW view_weibo_stats_increment AS
SELECT
    u.user_name,
    s.recorded_at,
    s.followers_count,
    COALESCE(
      s.followers_count - LAG(s.followers_count) OVER (PARTITION BY s.user_id ORDER BY s.recorded_at),
      0
    ) AS followers_increment
FROM weibo_user_stats s
JOIN weibo_user u ON s.user_id = u.user_id;

--Create a view of increment of weibo users every day
--DROP VIEW IF EXISTS view_weibo_daily_increment;
CREATE OR REPLACE VIEW view_weibo_daily_increment AS
WITH daily_snapshot AS (
    SELECT
        user_id,
        DATE(recorded_at) AS stat_date,
        MAX(recorded_at) AS last_recorded_at
    FROM weibo_user_stats
	WHERE recorded_at >= TIMESTAMP '2025-07-13 04:00:00'
    GROUP BY user_id, DATE(recorded_at)
),
daily_followers AS (
    SELECT
        s.user_id,
        DATE(s.recorded_at) AS stat_date,
        s.followers_count
    FROM weibo_user_stats s
    JOIN daily_snapshot d
      ON s.user_id = d.user_id
     AND s.recorded_at = d.last_recorded_at
)
SELECT
    u.user_name,
    d.stat_date,
    d.followers_count,
    COALESCE(
        d.followers_count - LAG(d.followers_count) OVER (PARTITION BY d.user_id ORDER BY d.stat_date),
        0
    ) AS daily_increment
FROM daily_followers d
JOIN weibo_user u ON d.user_id = u.user_id
ORDER BY u.user_name, d.stat_date;

--Create a view of how many low_rating=1 users have been fetched rated dramas
--DROP VIEW IF EXISTS view_fetched_low_rating_related_users;

CREATE OR REPLACE VIEW view_fetched_low_rating_related_users AS
SELECT COUNT(DISTINCT user_id)
FROM (
    -- 1. Appears in drama_collection and has been recorded in the low_rating_users table
    SELECT DISTINCT dc.user_id
    FROM drama_collection dc
    JOIN (
        SELECT DISTINCT user_id
        FROM low_rating_users
        WHERE drama_id = '36317401'
    ) AS lru
    ON dc.user_id = lru.user_id

    UNION

    -- 2. Select the reviewers with fetched=true directly from low_rating_users
    SELECT user_id
    FROM low_rating_users
    WHERE drama_id = '36317401' AND fetched = true
) AS combined_user_ids;




--Create a view of all fetched low_rating users of zhaoxuelu
CREATE OR REPLACE VIEW view_fetched_users_in_low_rating_36317401 AS
SELECT COUNT(DISTINCT f.user_id) AS fetched_user_id
FROM fetched_users f
JOIN (
    SELECT DISTINCT user_id
    FROM low_rating_users
    WHERE drama_id = '36317401'
) AS lru
ON f.user_id = lru.user_id;


-- Create a view of how iqiyi hot rank changes over time
--DROP VIEW IF EXISTS view_zhaoxuelu_iqiyi_hotlink_with_shanghai_time;

CREATE OR REPLACE VIEW view_zhaoxuelu_iqiyi_hotlink_with_shanghai_time AS
SELECT 
	insert_time,
    insert_time AT TIME ZONE 'Asia/Shanghai' AS insert_time_shanghai,
    hot_link
FROM public.zhaoxuelu_heat_iqiyi
WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00';

--Create a view of how many users whose collection have been fetched really(not skip, total amount <=500)
CREATE OR REPLACE VIEW view_users_with_collection AS
SELECT COUNT(DISTINCT user_id)
FROM drama_collection dc;


--Create a view of calculate group topics about zhaoxuelu
CREATE OR REPLACE VIEW view_zhaoxuelu_topics_groups AS
SELECT
    group_name,
    COUNT(*) AS topic_count
FROM
    zhaoxuelu_group_topics
GROUP BY
    group_name
ORDER BY
    topic_count DESC;
	
--Create a view of board of hot search and TV rank 热搜榜，电视剧榜	
CREATE OR REPLACE VIEW view_iqiyi_hotsearch_ranking AS
SELECT
    ranking,
    title,
    category,
    collected_at
FROM
    iqiyi_rank_titles
WHERE
    batch_id = (
        SELECT batch_id
        FROM iqiyi_rank_titles
        ORDER BY collected_at DESC
        LIMIT 1
    )
ORDER BY
    ranking ASC;

--热播榜
CREATE OR REPLACE VIEW view_iqiyi_tv_ranking AS
SELECT
    order_index,
    title,
	main_index,
    collected_at
FROM
    iqiyi_rank_dramas
WHERE
    batch_id = (
        SELECT batch_id
        FROM iqiyi_rank_dramas
        ORDER BY collected_at DESC
        LIMIT 1
    )
ORDER BY
    order_index ASC;


