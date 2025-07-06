-- Find the dramas with the highest ratings from these low-scoring users

SELECT COUNT(user_id) AS high_rating_count, drama_id
FROM drama_collection
WHERE source_drama_id = '36744438' AND rating = 5
GROUP BY drama_id
ORDER BY high_rating_count DESC;

