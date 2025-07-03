--daram name: filter, zhaoxuelu,lizhi，huanyu,shujuanyimeng


-- Create table 'zhaoxuelu_comments' to store user comments on the drama 'Zhaoxuelu'.
-- Ensures uniqueness by combining user_id and comment timestamp (create_time).
CREATE TABLE public.linjiangxian_comments (
    user_id VARCHAR(20) NOT NULL,
    user_name VARCHAR(60),
    votes INTEGER,
    status VARCHAR(10),
    rating INTEGER,
    user_location VARCHAR(20),
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    user_comment TEXT,
    insert_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    
    CONSTRAINT unique_linjiangxian_user_time UNIQUE (user_id, create_time)
);

--Create commen index
CREATE INDEX idx_user_zhaoxuelu ON zhaoxuelu_comments(user_id);



--Delete data in the table

TRUNCATE TABLE public.zhaoxuelu_comments;

--Delete table

DROP TABLE public.lizhi_comments;

-- Create 'filter_comments' table with the same structure as 'zhaoxuelu_comments'
-- (includes all columns, constraints, and indexes).

CREATE TABLE public.filter_comments (LIKE public.zhaoxuelu_comments INCLUDING ALL);


-- Check if there is CONSTRAINT
SELECT conname, contype 
FROM pg_constraint 
WHERE conname = 'unique_shujuanyimeng_user_time';


-- Add again CONSTRAINT
ALTER TABLE filter_comments DROP CONSTRAINT IF EXISTS unique_user_time;

ALTER TABLE filter_comments 
ADD CONSTRAINT unique_filter_user_time UNIQUE (user_id, create_time);



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



ALTER TABLE shujuanyimeng_comments_count
ALTER COLUMN insert_time TYPE TIMESTAMP WITH TIME ZONE
USING insert_time AT TIME ZONE 'UTC';

ALTER TABLE shujuanyimeng_comments_count
ALTER COLUMN insert_time SET DEFAULT now();


-- Save the users who give low scores (2 stars and below), 
-- and mark whether her collect has been fetched, 
-- to facilitate deduplication, management, and breakpoint resumption
CREATE TABLE low_rating_users (
  user_id VARCHAR(20) NOT NULL,
  drama_id VARCHAR(20) NOT NULL, 
  rating INT,
  comment_time TIMESTAMP WITH TIME ZONE NOT NULL,
  fetched BOOLEAN DEFAULT FALSE,  
  fetched_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
  PRIMARY KEY (user_id, drama_id)
);

DROP TABLE low_rating_users;

-- The first time insert data from comment table
INSERT INTO low_rating_users (user_id, drama_id, rating, comment_time)
SELECT DISTINCT user_id, '36744438', rating, create_time
FROM shujuanyimeng_comments
WHERE rating <= 2
ON CONFLICT (user_id, drama_id) DO NOTHING;


--Create table of collection of dramas of all users
CREATE TABLE drama_collection (
  source_drama_id VARCHAR(20),
  user_id VARCHAR(20) NOT NULL,
  drama_id VARCHAR(20) NOT NULL, 
  rating INTEGER,
  rating_time TIMESTAMP WITH TIME ZONE NOT NULL,
  comment TEXT,  
  vote_useful INTEGER,
  insert_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
  PRIMARY KEY (user_id, drama_id)
);


DROP TABLE drama_collection;


-- Control the "user-level" crawling process to avoid unnecessary repeated crawling
CREATE TABLE fetched_users (
    user_id VARCHAR(20) PRIMARY KEY,
    fetched_time TIMESTAMP WITH TIME ZONE DEFAULT now()
);


