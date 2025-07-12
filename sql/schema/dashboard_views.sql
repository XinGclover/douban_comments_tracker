-- Create a view of how iqiyi heat changes over time
CREATE OR REPLACE VIEW view_zhaoxuelu_heat_iqiyi_with_shanghai_time AS
SELECT 
    insert_time AT TIME ZONE 'Asia/Shanghai' AS insert_time_shanghai,
    heat_info
FROM public.zhaoxuelu_heat_iqiyi
ORDER by insert_time DESC;
	
	
-- Create a view of how douban total_comments，total_reviews，total_discussions changes over time
CREATE OR REPLACE VIEW view_zhaoxuelu_comments_count_with_shanghai_time AS
SELECT 
    insert_time AT TIME ZONE 'Asia/Shanghai' AS insert_time_shanghai,
	total_comments,
	total_reviews,
	total_discussions
FROM public.zhaoxuelu_comments_count
ORDER by insert_time DESC;


-- Create a view of all ratings percentage
CREATE OR REPLACE VIEW view_zhaoxuelu_comments_rating_percentage AS
SELECT 
   rating, 
   count(*) as rating_count, 
   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS rating_percent
FROM public.zhaoxuelu_comments
GROUP by rating
ORDER by rating DESC;


-- Create a view of distribution of different ratings in different regions
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
CREATE OR REPLACE VIEW view_zhaoxuelu_iqiyiheat_timeline AS
SELECT 
    to_char(timezone('Asia/Shanghai', insert_time), 'HH24:MI:SS') AS time_part,
    date(timezone('Asia/Shanghai', insert_time)) AS date_part,
    heat_info
FROM zhaoxuelu_heat_iqiyi
WHERE date(timezone('Asia/Shanghai', insert_time)) >= '2025-06-25';

--DROP VIEW IF EXISTS view_zhaoxuelu_iqiyiheat_timeline;


--Create a view of high rating dramas by low rating users for one source drama
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


--Create a view of all high frequency words
CREATE OR REPLACE VIEW view_zhaoxuelu_top_words AS
SELECT 
    word,
    COUNT(*) AS freq
FROM zhaoxuelu_comment_words
GROUP BY word
ORDER BY freq DESC
LIMIT 100;


--Create a view of daily word frequency trend
CREATE OR REPLACE VIEW view_zhaoxuelu_daily_top_words AS
SELECT 
    DATE_TRUNC('day', create_time) AS day,
    word,
    COUNT(*) AS freq
FROM zhaoxuelu_comment_words
GROUP BY day, word
ORDER BY day, freq DESC;


--Create a view of trend of weibo users
CREATE OR REPLACE VIEW view_weibo_stats AS
SELECT
    u.user_name,
    s.followers_count,
    s.likes_count,
	s.recorded_at
FROM weibo_user_stats s
JOIN weibo_user u ON s.user_id = u.user_id;

