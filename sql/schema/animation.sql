CREATE OR REPLACE VIEW view_zhaoxuelu_topics_groups_with_time AS
SELECT
    group_name,
	full_time,
    COUNT(*) AS topic_count
FROM
    zhaoxuelu_group_topics
GROUP BY
    group_name,full_time
ORDER BY
    topic_count DESC;
	
	
	
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