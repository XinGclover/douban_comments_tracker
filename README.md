# ❄️ Drama Comment Tracking and Analysis Project #

This project aims to continuously scrape the latest short comments and user information for the Chinese drama from Douban, store them in a local PostgreSQL database, and later analyze user rating behavior to detect astroturfing activities, build fan profiles, and track public opinion trends.

### 🎯 Project Objectives ###
⏱ High-frequency scraping: Scrape the latest Douban short comments every hour starting from the show's release date
👤 User profiling: Record user IDs and fetch their historical rating activity
🕵️‍♀️ Astroturfing detection: Analyze patterns in ratings, comment timing, and text to identify fake or coordinated accounts
📊 Trend analysis: Visualize public opinion dynamics, sentiment shifts, and topic heat over time

### 🧪 Current Features ###
#### ⏱ Automated Hourly Scraping ####
Automatically scrapes the latest short comments from Douban every hour using a scheduled task.
#### 💬 Comment & User Data Extraction ####
Collects key fields including comment content, user ID, comment timestamp, and number of likes.

### 🗃 Data Storage in PostgreSQL ###
All collected data is saved into a structured PostgreSQL database for further analysis.
#### 🔍 User Rating Behavior Tracking (in development) ####
Fetches historical rating records of commenting users to build detailed user profiles.
#### 🕵️‍♂️ Astroturfing Detection & Analysis (in development) ####
Uses temporal patterns, user activity, and comment sentiment to identify potential spam or coordinated behavior (a.k.a. "water army").


### 🧱 Tech Stack ###
* Python – Core scraping and data logic (with requests, BeautifulSoup, pandas)
* PostgreSQL – Relational database for structured storage
* Airflow (planned) – For hourly task scheduling and monitoring
* Jupyter Notebook – Used for exploratory data analysis and prototyping
* Looker Studio / Tableau (optional) – For dashboard creation and visualizations

### 🗃 Database Schema ###

| Field Name         | Type              | Description                                                               |
| ------------------ | ----------------- | ------------------------------------------------------------------------- |
| `user_id`          | BIGINT            | Unique identifier of the user (not null)                                  |
| `user_name`        | VARCHAR(60)       | Display name of the user                                                  |
| `votes`            | INTEGER           | Number of likes received by the comment                                   |
| `status`           | VARCHAR(10)       | Viewing status (e.g., "Watched", "Want to watch")                         |
| `rating`           | INTEGER           | User's rating for the show (1 to 5), nullable                             |
| `user_location`    | VARCHAR(20)       | Reported location of the user (if available)                              |
| `create_time`      | TIMESTAMP         | Timestamp when the comment was posted (not null)                          |
| `user_comment`     | TEXT              | Content of the user's comment                                             |
| `insert_time`      | TIMESTAMP         | Timestamp when the data was inserted into the database (default: `now()`) |
| `unique_user_time` | UNIQUE CONSTRAINT | Ensures no duplicate records for the same user and comment time           |



## Disclaimer

This project is intended **for educational and research purposes only**.  
The code is provided "as is" without any guarantees or warranties.

**Important:**  
- This repository does **not** include or distribute any data scraped from third-party websites.  
- Users are responsible for complying with the terms of service and robots.txt policies of any websites they scrape.  
- The author does not encourage or endorse any unlawful or unethical use of this code.

By using this code, you agree to use it responsibly and ethically.
