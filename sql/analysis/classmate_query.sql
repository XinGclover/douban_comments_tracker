with landin_dramas as (
 select drama_id from douban_drama_info WHERE actors::text LIKE '%李兰迪%'
),
no_good_ppl as (
 select lru.user_id, lru.drama_id, avg(lru.rating) as landin_rating
 from low_rating_users lru
 join landin_dramas ld on ld.drama_id = lru.drama_id
 group by lru.user_id, lru.drama_id
),
avg_ratings as (
 select lru.user_id, avg(lru.rating) as avg_normal_rating 
 from low_rating_users lru
 join no_good_ppl ngp on ngp.user_id = lru.user_id
 group by lru.user_id
)
select ngp.*, ar.*, tpr.content_text as landin_comment
from no_good_ppl ngp
join avg_ratings ar on ar.user_id = ngp.user_id
left join douban_topic_post_raw tpr on tpr.user_id = ngp.user_id
--where ngp.landin_rating < ar.avg_normal_rating



--hitta alla som gett Landis dramas lägre betyg än deras snitt 
with landi_dramas as (
 select drama_id from douban_drama_info where actors::text LIKE '%李兰迪%'
),
no_good_ppl as (
 select lru.user_id, lru.drama_id, avg(lru.rating) as landin_rating
 from low_rating_users lru
 join landi_dramas ld on ld.drama_id = lru.drama_id
 group by lru.user_id, lru.drama_id
),
avg_ratings as (
 select lru.user_id, cast(avg(lru.rating) as decimal(2,1)) as avg_normal_rating, count(*) as count_other_ranks
 from low_rating_users lru
 where not exists (select 1 from landi_dramas ld where ld.drama_id = lru.drama_id)
 group by lru.user_id
)
select ngp.*, ar.*, tpr.content_text as landin_comment
from no_good_ppl ngp
join avg_ratings ar on ar.user_id = ngp.user_id
left join douban_topic_post_raw tpr on tpr.user_id = ngp.user_id
where ngp.landin_rating < ar.avg_normal_rating

-- right table 
with landi_dramas as (
 select drama_id,drama_name from douban_drama_info where actors::text LIKE '%李兰迪%'
),
no_good_ppl as (
 select lru.user_id, lru.drama_id, avg(lru.rating) as landin_rating
 from drama_collection lru
 join landi_dramas ld on ld.drama_id = lru.drama_id
 group by lru.user_id, lru.drama_id
),
avg_ratings as (
 select lru.user_id, cast(avg(lru.rating) as decimal(2,1)) as avg_normal_rating, count(*) as count_other_ranks
 from low_rating_users lru
 where not exists (select 1 from landi_dramas ld where ld.drama_id = lru.drama_id)
 group by lru.user_id
)
select ngp.*, ar.*, tpr.content_text as landin_comment
from no_good_ppl ngp
join avg_ratings ar on ar.user_id = ngp.user_id
left join douban_topic_post_raw tpr on tpr.user_id = ngp.user_id
--where ngp.landin_rating < ar.avg_normal_rating
where ngp.user_id = '231450710'


 SELECT drama_id, COUNT(DISTINCT user_id) AS user_count
    FROM drama_collection
    WHERE rating = 5
    AND drama_id IN (SELECT drama_id FROM high_rating_dramas_from_low_rating_users)
    AND NOT EXISTS (
        SELECT 1 FROM douban_drama_info d
        WHERE d.drama_id = drama_collection.drama_id
    )
    GROUP BY drama_id
    ORDER BY user_count DESC
	
select * from douban_drama_info where actors::text LIKE '%李兰迪%'

WITH landi_low_raters AS (
    SELECT DISTINCT lru.user_id
    FROM low_rating_users lru
    WHERE lru.drama_id IN (
        SELECT drama_id FROM douban_drama_info 
        where actors::text LIKE '%李兰迪%'
    )
),
group_overlap AS (
    SELECT 
        dgm.group_id,
        dg.group_name,
        dg.group_who,           -- the actor/show the group is named after
        COUNT(DISTINCT dgm.member_id)   AS landi_haters_in_group,
        COUNT(DISTINCT dgm2.member_id)  AS total_group_members
    FROM landi_low_raters llr
    JOIN douban_group_members dgm  ON dgm.member_id = llr.user_id
    JOIN douban_groups dg           ON dg.group_id  = dgm.group_id
    -- get total members for that group (not just haters)
    JOIN douban_group_members dgm2  ON dgm2.group_id = dgm.group_id
    GROUP BY dgm.group_id, dg.group_name, dg.group_who
)
SELECT 
    group_id,
    group_name,
    group_who,
    landi_haters_in_group,
    total_group_members,
    CAST(landi_haters_in_group AS DECIMAL(5,2)) 
        / NULLIF(total_group_members, 0) * 100  AS pct_are_landi_haters
FROM group_overlap
ORDER BY landi_haters_in_group DESC, pct_are_landi_haters DESC;


WITH landi_dramas AS (
    SELECT
        drama_id,
        drama_name
    FROM douban_drama_info
    --WHERE '李兰迪' = ANY(actors)
),

user_landi_ratings AS (
    SELECT
        dc.user_id,
        dc.drama_id,
        ld.drama_name,
        dc.rating AS landi_rating
    FROM drama_collection dc
    JOIN landi_dramas ld
        ON ld.drama_id = dc.drama_id
    --WHERE dc.user_id = '222984488'
),

user_other_avg AS (
    SELECT
        dc.user_id,
        ROUND(AVG(dc.rating)::numeric, 2) AS avg_other_rating,
        COUNT(*) AS other_drama_count
    FROM drama_collection dc
    WHERE dc.user_id = '222984488'
      AND dc.drama_id NOT IN (
          SELECT drama_id FROM landi_dramas
      )
    GROUP BY dc.user_id
)

SELECT
    ulr.user_id,
    ulr.drama_id,
    ulr.drama_name,
    ulr.landi_rating,
    uoa.avg_other_rating,
    uoa.other_drama_count,
    ROUND((ulr.landi_rating - uoa.avg_other_rating)::numeric, 2) AS rating_diff_vs_other_avg
FROM user_landi_ratings ulr
LEFT JOIN user_other_avg uoa
    ON uoa.user_id = ulr.user_id
ORDER BY ulr.landi_rating DESC, ulr.drama_name