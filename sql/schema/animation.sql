CREATE OR REPLACE VIEW view_cumulative_group_topic_per_day AS
WITH date_series AS (
    -- All dates (from earliest full_time to latest)
    SELECT generate_series(
        (SELECT MIN(full_time::date) FROM zhaoxuelu_group_topics),
        (SELECT MAX(full_time::date) FROM zhaoxuelu_group_topics),
        interval '1 day'
    )::date AS topic_day
),
all_groups AS (
    -- All the groups
    SELECT DISTINCT ON (group_id)
        group_id,
        group_name
	FROM zhaoxuelu_group_topics
),
date_group_matrix AS (
    -- Complete combination of day × group
    SELECT
        d.topic_day,
        g.group_id,
        g.group_name
    FROM date_series d
    CROSS JOIN all_groups g
),
daily_counts AS (
    -- Actual daily posting volume
    SELECT
        full_time::date AS topic_day,
        group_id,
        COUNT(*) AS daily_topic_count
    FROM zhaoxuelu_group_topics
    GROUP BY group_id, topic_day
),
merged AS (
    -- Fill missing days with 0
    SELECT
        m.topic_day,
        m.group_id,
        m.group_name,
        COALESCE(d.daily_topic_count, 0) AS daily_topic_count
    FROM date_group_matrix m
    LEFT JOIN daily_counts d
    ON m.group_id = d.group_id AND m.topic_day = d.topic_day
),
cumulative AS (
    SELECT
        topic_day,
        group_id,
        group_name,
        SUM(daily_topic_count) OVER (
            PARTITION BY group_id
            ORDER BY topic_day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_topic_count
    FROM merged
)
SELECT * FROM cumulative;



--2025.08.04 version, but some of the latest values are 0,
--can fix in Flourish-Number formatting Invalid cells-use last valid
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

--2025.08.05 version, fix some dramas disappear bug
CREATE OR REPLACE VIEW view_continuous_top20_dramas_per_day AS
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
ranked_per_day AS (
    SELECT
        comment_day,
        drama_name,
        cumulative_count,
        ROW_NUMBER() OVER (
            PARTITION BY comment_day
            ORDER BY cumulative_count DESC
        ) AS rank
    FROM cumulative_counts
),
top20_history AS (
    SELECT DISTINCT drama_name
    FROM ranked_per_day
    WHERE rank <= 20
),
all_dates AS (
    SELECT DISTINCT comment_day FROM view_high_rating_dramas_source_zhaoxuelu_with_time
),
date_drama_grid AS (
    SELECT d.comment_day, t.drama_name
    FROM all_dates d
    CROSS JOIN top20_history t
),
joined AS (
    SELECT
        g.comment_day,
        g.drama_name,
        r.cumulative_count
    FROM date_drama_grid g
    LEFT JOIN cumulative_counts r
        ON g.comment_day = r.comment_day AND g.drama_name = r.drama_name
),
filled AS (
    SELECT
        comment_day,
        drama_name,
        -- Fill the accumulated value forward with LAG, the first fill
        COALESCE(
            cumulative_count,
            LAG(cumulative_count) OVER (
                PARTITION BY drama_name ORDER BY comment_day
            )
        ) AS filled_cumulative_count
    FROM joined
),
-- Recursive filling is omitted here, and can be supplemented or run multiple times on the client side.
ranked_final AS (
    SELECT
        comment_day,
        drama_name,
        filled_cumulative_count AS cumulative_count,
        ROW_NUMBER() OVER (
            PARTITION BY comment_day
            ORDER BY filled_cumulative_count DESC NULLS LAST
        ) AS rank
    FROM filled
)
SELECT *
FROM ranked_final
WHERE rank <= 20
ORDER BY comment_day, rank;






-- Create a view of all ratings percentage changes dayily
CREATE OR REPLACE VIEW view_zhaoxuelu_comments_rating_percentage_daily AS
WITH daily_rating_count AS (
    SELECT 
        DATE(insert_time) AS comment_date,
        rating,
        COUNT(*) AS rating_count
    FROM public.zhaoxuelu_comments
    WHERE insert_time >= TIMESTAMP '2025-07-13 04:00:00'
    GROUP BY DATE(insert_time), rating
),
-- Cumulative number of all dates + ratings
cumulative_rating AS (
    SELECT
        comment_date,
        rating,
        SUM(rating_count) OVER (PARTITION BY rating ORDER BY comment_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_count
    FROM daily_rating_count
),
-- Total number of reviews per day (all ratings added up)
cumulative_total AS (
    SELECT
        comment_date,
        SUM(cumulative_rating_count) AS cumulative_total_comments
    FROM cumulative_rating
    GROUP BY comment_date
)
-- Final output: The proportion of each rating in the cumulative reviews per day
SELECT
    r.comment_date,
    r.rating,
    r.cumulative_rating_count,
    t.cumulative_total_comments,
    ROUND(r.cumulative_rating_count * 100.0 / t.cumulative_total_comments, 2) AS cumulative_percent
FROM cumulative_rating r
JOIN cumulative_total t USING (comment_date)
ORDER BY r.comment_date, r.rating DESC;
