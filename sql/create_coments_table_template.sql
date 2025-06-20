-- Create table 'zhaoxuelu_comments' to store user comments on the drama 'Zhaoxuelu'.
-- Ensures uniqueness by combining user_id and comment timestamp (create_time).

CREATE TABLE public.lizhi_comments (
    user_id VARCHAR(20) NOT NULL,
    user_name VARCHAR(60),
    votes INTEGER,
    status VARCHAR(10),
    rating INTEGER,
    user_location VARCHAR(20),
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    user_comment TEXT,
    insert_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    
    CONSTRAINT unique_lizhi_user_time UNIQUE (user_id, create_time)
);

--Delete data in the table

TRUNCATE TABLE public.zhaoxuelu_comments;

--Delete table

DROP TABLE public.lizhi_comments;

-- Create 'filter_comments' table with the same structure as 'zhaoxuelu_comments'
-- (includes all columns, constraints, and indexes).

CREATE TABLE public.filter_comments (LIKE public.zhaoxuelu_comments INCLUDING ALL);


-- Change user_id type from BIGINT to VARCHAR
ALTER TABLE lizhi_comments
ALTER COLUMN user_id TYPE VARCHAR(20);


-- Check if there is CONSTRAINT
SELECT conname, contype 
FROM pg_constraint 
WHERE conname = 'unique_lizhi_user_time';


-- Add again CONSTRAINT
ALTER TABLE filter_comments DROP CONSTRAINT IF EXISTS unique_user_time;

ALTER TABLE filter_comments 
ADD CONSTRAINT unique_filter_user_time UNIQUE (user_id, create_time);



