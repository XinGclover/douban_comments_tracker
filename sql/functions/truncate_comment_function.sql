--Automatically truncates the 'user_comment' field to 100 characters 
--if it exceeds that length, before inserting into the table.

CREATE OR REPLACE FUNCTION truncate_to_100_chars()
RETURNS TRIGGER AS $$
BEGIN
    NEW.comment := LEFT(NEW.comment, 100);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Invokes the truncate_long_comment() function before each INSERT

CREATE TRIGGER trg_truncate_full_text
BEFORE INSERT OR UPDATE ON drama_collection
FOR EACH ROW
EXECUTE FUNCTION truncate_to_100_chars();



-------------------------------------------------------------

--First check what triggers are on this table
SELECT tgname
FROM pg_trigger
WHERE tgrelid = 'zhaoxuelu_comments'::regclass;

-- Deleting a trigger
DROP TRIGGER IF EXISTS trg_truncate_full_text ON zhaoxuelu_comments;
