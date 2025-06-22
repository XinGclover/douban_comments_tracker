SELECT AVG(rating)
FROM public.lizhi_comments;



SELECT *
FROM public.lizhi_comments
WHERE rating = 1
ORDER by create_time DESC;



SELECT *
FROM public.filter_comments
WHERE user_id = 261865969;


SELECT count(*) AS count,user_location
FROM public.lizhi_comments
WHERE rating = 5
GROUP by user_location
ORDER by count DESC;


SELECT *
FROM public.lizhi_comments
ORDER by create_time DESC;

