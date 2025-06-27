--Create table to store daily summary statistics of the Douban drama 

CREATE TABLE public.linjiangxian_comments_count (
	insert_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now(), 
	rating NUMERIC(3,1),
	rating_people INTEGER,                        -- Number of people who rated
    rating_1_star NUMERIC(5,2),           	      -- 1 star ratio
    rating_2_star NUMERIC(5,2),                   -- 2 star ratio
	rating_3_star NUMERIC(5,2),                   -- 3 star ratio
	rating_4_star NUMERIC(5,2),                   -- 4 star ratio
	rating_5_star NUMERIC(5,2),                   -- 5 star ratio
    total_comments INTEGER,                       -- Number of short comments
    total_reviews INTEGER,                        -- Number of long comments
    total_discussions INTEGER,                    -- Discussion group size    
	
	CONSTRAINT linjiangxian_unique_time UNIQUE (insert_time)
);

DROP TABLE public.huanyu_comments_count;

--daram name: filter, zhaoxuelu,lizhi，huanyu



ALTER TABLE shujuanyimeng_comments_count
ALTER COLUMN insert_time TYPE TIMESTAMP WITH TIME ZONE
USING insert_time AT TIME ZONE 'UTC';

ALTER TABLE shujuanyimeng_comments_count
ALTER COLUMN insert_time SET DEFAULT now();

