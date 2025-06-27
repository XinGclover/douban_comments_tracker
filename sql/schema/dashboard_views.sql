-- Create a view of how iqiyi heat changes over time
CREATE VIEW view_shujuanyimeng_heat_iqiyi_with_shanghai_time AS
SELECT 
    insert_time AT TIME ZONE 'Asia/Shanghai' AS insert_time_shanghai,
    heat_info
FROM public.shujuanyimeng_heat_iqiyi
ORDER by insert_time DESC;
	
	
-- Create a view of how douban total_comments，total_reviews，total_discussions changes over time
CREATE VIEW view_shujuanyimeng_comments_count_with_shanghai_time AS
SELECT 
    insert_time AT TIME ZONE 'Asia/Shanghai' AS insert_time_shanghai,
	total_comments,
	total_reviews,
	total_discussions
FROM public.shujuanyimeng_comments_count
ORDER by insert_time DESC;


-- Create a view of all ratings percentage
CREATE VIEW view_shujuanyimeng_comments_rating_percentage AS
SELECT 
   rating, 
   count(*) as rating_count, 
   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS rating_percent
FROM public.shujuanyimeng_comments
GROUP by rating
ORDER by rating DESC;


-- Create a view of distribution of different ratings in different regions
CREATE OR REPLACE VIEW view_shujuanyimeng_comments_distribution AS
SELECT 
    user_location,
    COUNT(*) FILTER (WHERE rating = 1) AS rating_1_count,
    COUNT(*) FILTER (WHERE rating = 2) AS rating_2_count,
    COUNT(*) FILTER (WHERE rating = 3) AS rating_3_count,
    COUNT(*) FILTER (WHERE rating = 4) AS rating_4_count,
    COUNT(*) FILTER (WHERE rating = 5) AS rating_5_count,
	COUNT(*) FILTER (WHERE rating IS NULL) AS no_rating_count,
    COUNT(*) AS total_count
FROM public.shujuanyimeng_comments
GROUP by user_location
ORDER by total_count DESC;


--Create a long view of distribution of different ratings in different regions
CREATE OR REPLACE VIEW view_shujuanyimeng_comments_distribution_long AS
SELECT user_location, 'rating_1' AS rating_category, rating_1_count AS count FROM view_shujuanyimeng_comments_distribution
UNION ALL
SELECT user_location, 'rating_2' AS rating_category, rating_2_count AS count FROM view_shujuanyimeng_comments_distribution
UNION ALL
SELECT user_location, 'rating_3' AS rating_category, rating_3_count AS count FROM view_shujuanyimeng_comments_distribution
UNION ALL
SELECT user_location, 'rating_4' AS rating_category, rating_4_count AS count FROM view_shujuanyimeng_comments_distribution
UNION ALL
SELECT user_location, 'rating_5' AS rating_category, rating_5_count AS count FROM view_shujuanyimeng_comments_distribution
UNION ALL
SELECT user_location, 'no_rating' AS rating_category, no_rating_count AS count FROM view_shujuanyimeng_comments_distribution;




	
	
