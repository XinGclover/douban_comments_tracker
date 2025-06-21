SELECT AVG(rating)
FROM public.filter_comments;



SELECT *
FROM public.filter_comments
WHERE rating = 1
ORDER by user_location;



SELECT *
FROM public.filter_comments
WHERE user_id = 261865969;


SELECT count(*) AS count,user_location
FROM public.filter_comments
WHERE rating = 5
GROUP by user_location
ORDER by count DESC;
