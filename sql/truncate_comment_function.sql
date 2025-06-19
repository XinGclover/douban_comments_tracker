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
