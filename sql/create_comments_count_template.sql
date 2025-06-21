--Create table to store daily summary statistics of the Douban drama 

CREATE TABLE public.zhaoxuelu_comments_count (
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
	
	CONSTRAINT zhaoxuelu_unique_time UNIQUE (insert_time)
);

DROP TABLE public.zhaoxuelu_comments_count;

--table name: filter_comments_count, zhaoxuelu_comments_count,lizhi_comments_count
--constraint name: filter_unique_time, zhaoxuelu_unique_time,lizhi_unique_time




