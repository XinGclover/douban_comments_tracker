-- Queries for streamlit function
--Create a table of watchlist to record the user_id I queried
-- DROP TABLE person_watchlist
CREATE TABLE IF NOT EXISTS person_watchlist (
    person_id VARCHAR(50) PRIMARY KEY,
    first_seen TIMESTAMP DEFAULT NOW(),
    last_seen TIMESTAMP DEFAULT NOW(),
    query_count INT DEFAULT 1
);
