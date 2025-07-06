--SQL example for water army detection
--total_ratings > 100: Rated more than 100 dramas, a large number, may be to maintain the account to accumulate points
--rating_range < 3: The rating span is small (the ratings are very close), abnormal
--active_days < 10: The ratings are concentrated within 10 days, very dense
--avg_ratings_per_day > 20: More than 20 ratings per day, high frequency of ratings

WITH user_stats AS (
  SELECT
    user_id,
    COUNT(*) AS total_ratings,
    MAX(rating) - MIN(rating) AS rating_range,
    COUNT(DISTINCT DATE(rating_time)) AS active_days,
    MIN(rating_time) AS first_rating,
    MAX(rating_time) AS last_rating,
    ROUND(EXTRACT(EPOCH FROM MAX(rating_time) - MIN(rating_time)) / 86400) AS active_span_days,
    -- Calculate the average rating per day
    ROUND(COUNT(*)::float / NULLIF(COUNT(DISTINCT DATE(rating_time)),0)) AS avg_ratings_per_day
  FROM
    drama_collection
  GROUP BY
    user_id
),

suspicious_users AS (
  SELECT
    user_id,
    total_ratings,
    rating_range,
    active_days,
    active_span_days,
    avg_ratings_per_day,
    -- Determine whether it is suspected to be a water army
    CASE 
      WHEN total_ratings > 100 AND rating_range < 3 AND active_days < 10 THEN 'High Suspicion'
      WHEN avg_ratings_per_day > 20 THEN 'High Suspicion'
      WHEN total_ratings > 200 AND rating_range < 5 THEN 'Medium Suspicion'
      ELSE 'Low Suspicion'
    END AS suspicion_level
  FROM user_stats
)

SELECT * FROM suspicious_users
ORDER BY suspicion_level DESC, total_ratings DESC;
