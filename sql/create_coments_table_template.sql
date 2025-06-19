-- Create table 'zhaoxuelu_comments' to store user comments on the drama 'Zhaoxuelu'.
-- Ensures uniqueness by combining user_id and comment timestamp (create_time).

CREATE TABLE public.zhaoxuelu_comments (
    user_id BIGINT NOT NULL,
    user_name VARCHAR(60),
    votes INTEGER,
    status VARCHAR(10),
    rating INTEGER,
    user_location VARCHAR(20),
    create_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    user_comment TEXT,
    insert_time TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    
    CONSTRAINT unique_user_time UNIQUE (user_id, create_time)
);

--Automatically truncates the 'user_comment' field to 100 characters 
--if it exceeds that length, before inserting into the table.

CREATE OR REPLACE FUNCTION truncate_to_100_chars()
RETURNS TRIGGER AS $$
BEGIN
    NEW.user_comment := LEFT(NEW.user_comment, 100);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Invokes the truncate_long_comment() function before each INSERT

CREATE TRIGGER trg_truncate_full_text
BEFORE INSERT OR UPDATE ON zhaoxuelu_comments
FOR EACH ROW
EXECUTE FUNCTION truncate_to_100_chars();

--Delete data in the table

TRUNCATE TABLE public.zhaoxuelu_comments;

--Delete table

DROP TABLE public.zhaoxuelu_comments;

-- Create 'filter_comments' table with the same structure as 'zhaoxuelu_comments'
-- (includes all columns, constraints, and indexes).

CREATE TABLE public.filter_comments (LIKE public.zhaoxuelu_comments INCLUDING ALL);
