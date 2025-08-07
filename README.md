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
* Metabase – For dashboard creation and visualizations locally
* Kafka - Real-time streaming of short reviews; enables batch-wise low-rating detection and trigger actions.
    |

## Project Scripts & Command Reference
This README provides a structured overview of key scripts and commands used in the project, including environment setup, Airflow, Docker, and Kafka pipeline operations.

| Category          | Script/Command                                               | Description                                                                 | Example Usage                                 |
|-------------------|--------------------------------------------------------------|-----------------------------------------------------------------------------|-----------------------------------------------|
| Cookie Management | `update_cookie.py`                                           | Save cookies from a logged-in session to a file (run manually weekly)       | `python update_cookie.py`                      |
| Environment Setup | `env_airflow.sh`                                             | Configure the Airflow environment (must run in each terminal session)       | `source env_airflow.sh`                        |
| Airflow Users     | `airflow users list`                                         | Verify if an Airflow user exists                                            | Output includes `admin` user details           |
| Web UI            | `airflow webserver --port 8080`                              | Start Airflow Web Server UI                                                 | Open browser: `http://localhost:8080`          |
| Scheduler         | `airflow scheduler`                                          | Start Airflow Scheduler to trigger DAGs                                     | —                                              |
| PostgreSQL        | `brew services start postgresql@16`                          | Start PostgreSQL 16 as a background service (auto-start on boot)            | Check with `brew services list`                |
| Docker Logs       | `docker logs -f metabase`                                    | View real-time logs of the Metabase container                               | —                                              |
| Docker Start      | `docker start zhaoxuelu`                                     | Start a Docker container                                                    | —                                              |
| Docker Stop       | `docker stop zhaoxuelu`                                      | Stop a Docker container                                                     | —                                              |
| Docker Remove     | `docker rm -f metabase`                                      | Force remove a Docker container                                             | —                                              |
| Kafka Start       | `docker-compose up -d`                                       | Start Kafka and Zookeeper services via Docker                               | `cd kafka_docker && docker-compose up -d`      |
| Kafka Stop        | `bash scripts/stop_kafka.sh`                                 | Stop all Kafka and Zookeeper containers                                     | —                                              |
| Kafka Quick Start | `bash scripts/start_kafka.sh`                                | Shortcut to start all Kafka services                                        | Recommended startup method                     |

## Mind Map

![](/images/mindmap.png)


## Visualization Result

![](/images/zhaoxuelu.png)

![](/images/5rating_dramas.gif)


## Disclaimer

This project is intended **for educational and research purposes only**.  
The code is provided "as is" without any guarantees or warranties.

**Important:**  
- This repository does **not** include or distribute any data scraped from third-party websites.  
- Users are responsible for complying with the terms of service and robots.txt policies of any websites they scrape.  
- The author does not encourage or endorse any unlawful or unethical use of this code.

By using this code, you agree to use it responsibly and ethically.
