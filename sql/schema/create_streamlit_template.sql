-- Queries for streamlit function
--Create a table of watchlist to record the user_id I queried
CREATE TABLE IF NOT EXISTS member_watchlist (
    member_id VARCHAR(50) PRIMARY KEY,
    first_seen TIMESTAMP DEFAULT NOW(),
    last_seen TIMESTAMP DEFAULT NOW(),
    query_count INT DEFAULT 1
);
