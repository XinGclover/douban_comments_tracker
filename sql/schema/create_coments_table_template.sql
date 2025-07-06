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
  fetched_time TIMESTAMP WITH TIME ZONE DEFAULT NULL,   --2025.07.05 modify
  PRIMARY KEY (user_id, drama_id)
);

--2025.07.05 modify when user's collect is been fetched, insert fetch_time
ALTER TABLE low_rating_users ALTER COLUMN fetched_time DROP DEFAULT;

UPDATE low_rating_users
SET fetched_time = NULL
WHERE fetched = FALSE;

DROP TABLE low_rating_users;

-- Insert data from comment table
INSERT INTO low_rating_users (user_id, drama_id, rating, comment_time)
SELECT DISTINCT user_id, '36744438', rating, create_time
FROM shujuanyimeng_comments
WHERE rating <= 2
ON CONFLICT (user_id, drama_id) DO NOTHING;





--Fetch all rated dramas of low-scoring users
CREATE TABLE drama_collection (
  --source_drama_id VARCHAR(20),       2025.07.05 modify
  user_id VARCHAR(20) NOT NULL,
  drama_id VARCHAR(20) NOT NULL, 
  rating INTEGER,
  rating_time TIMESTAMP WITH TIME ZONE NOT NULL,
  comment TEXT,  
  vote_useful INTEGER,
  insert_time TIMESTAMP WITH TIME ZONE DEFAULT now(),
  PRIMARY KEY (user_id, drama_id)
);


--2025.07.05 modify, remove redundancy and confusion column
ALTER TABLE drama_collection DROP COLUMN source_drama_id;


DROP TABLE drama_collection;


-- Control the "user-level" crawling process to avoid unnecessary repeated crawling
CREATE TABLE fetched_users (
    user_id VARCHAR(20) PRIMARY KEY,
    fetched_time TIMESTAMP WITH TIME ZONE DEFAULT now()，
	total_dramas INTEGER
	inserted_dramas INTEGER
);


-- Create table douban_drama_info to store low_rating users' high rating dramas.

CREATE TABLE public.douban_drama_info (
	drama_id VARCHAR(20) PRIMARY KEY,
    drama_name VARCHAR(40),
	release_year INTEGER ,
	director TEXT,
	actors TEXT[],
	release_date DATE, 
	rating NUMERIC(3,1),
	rating_people INTEGER,                       
    rating_1_star NUMERIC(5,2),           	     
    rating_2_star NUMERIC(5,2),                   
	rating_3_star NUMERIC(5,2),                   
	rating_4_star NUMERIC(5,2),                   
	rating_5_star NUMERIC(5,2),                   
    total_comments INTEGER,                      
    total_reviews INTEGER,                        
    total_discussions INTEGER, 
	insert_time TIMESTAMP WITH TIME ZONE DEFAULT now()
);

TRUNCATE TABLE public.douban_drama_info;


-- Save the users who give high scores (5 stars )
CREATE TABLE high_rating_dramas_from_low_rating_users (
  drama_id VARCHAR(20) PRIMARY KEY
);

-- Jieba extracts comments from zhaoxuelu_comments table, 
-- does word segmentation, filters stop words, and stores them in a separate word list

CREATE TABLE zhaoxuelu_comment_words (
    user_id VARCHAR(20),
    create_time TIMESTAMP WITHOUT TIME ZONE,
    word TEXT
);

CREATE INDEX idx_word_zhaoxuelu ON zhaoxuelu_comment_words(word);
CREATE INDEX idx_time_zhaoxuelu ON zhaoxuelu_comment_words(create_time);







