# ❄️ Drama Comment Tracking and Analysis Project #

This project continuously scrapes the latest short comments and user information for a Chinese drama from Douban, stores them in a local PostgreSQL database, and applies AI-powered analysis to detect astroturfing activities, build fan behavior profiles, and track public opinion trends in real time.

### 🎯 Why This Project Matters ###
Online rating platforms are vulnerable to coordinated manipulation campaigns.
This project explores how to detect structured narrative amplification using:
* Semantic modeling (LLM)
* Community network analysis
* Temporal anomaly detection
* Behavioral clustering
It demonstrates end-to-end data engineering + AI integration in a real-world social signal problem.

### 🚀 Project Objectives ###
#### ⏱ High-frequency scraping: ####
Continuously scrape the latest Douban short comments every hour starting from the show’s release date. In addition, perform keyword-based topic discovery and crawl full threads (original posts + all replies) to capture complete discussion structures.
#### 👤 User profiling: ####
Track user IDs and reconstruct their historical rating timelines to build behavioral fingerprints. Users are further mapped to their Douban group memberships to analyze community affiliation and cross-group interaction patterns.
#### 🕵️‍♀️ Astroturfing detection: ####
Detect coordinated rating and comment manipulation by integrating temporal burst analysis, behavioral consistency modeling, and LLM-based semantic stance classification. Group affiliation bias is incorporated to assess whether collective negativity reflects organic disagreement or structured narrative amplification.
#### 📊 Trend analysis: ####
Model public opinion dynamics over time, including rating waves, sentiment shifts, and discussion heat evolution. Identify structural changes in narrative direction and cross-community polarization.
#### 🤖 AI-powered comment classification: ####
Use a locally hosted LLM pipeline to classify stance, sentiment polarity, sarcasm, and rebuttal behavior in structured JSON format. This semantic layer enables higher-level narrative and coordination analysis beyond keyword filtering.
#### 🖥 Interactive analytics console: ####
Provide a unified Streamlit-based control panel to execute scraping, AI analysis, and streaming pipelines. The console also enables parameterized SQL querying and interactive exploration of analytical database views.

#### 📊 Interactive Dashboard (Demo Layer) ####

A lightweight Streamlit dashboard has been built to showcase aggregated results from this data engineering pipeline:

🔗 **Live Demo:**  
https://douban-console-demo.streamlit.app

The dashboard is based on exported analytical tables from PostgreSQL and stored in a DuckDB file for demonstration purposes.

### 🧠 Key Technical Highlights ###
* Local LLM integration with structured JSON output
* Batch + streaming hybrid architecture
* Kafka-based low-rating burst detection
* Behavioral modeling across communities
* Layered data architecture (Bronze → Silver → Gold)
* Operational control console (Streamlit)

### 🧱 Tech Stack ###
| Layer         | Technology                       |
| ------------- | -------------------------------- |
| Scraping      | Python (requests, BeautifulSoup) |
| Storage       | PostgreSQL 16                    |
| AI            | Local LLM via Ollama             |
| Streaming     | Kafka                            |
| Scheduling    | Airflow                          |
| Visualization | Metabase                         |
| Control Panel | Streamlit                        |
| Analysis      | Pandas, SQL, Jupyter             |
----------------------------------------------------


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
| Steamlit Start    | `streamlit run streamlit_console/app.py`                     | Start a streamlit app                                                       | Open browser: `http://localhost:8501`
|

## System Architecture

![](/images/architecture.png)


## Visualization Result

![](/images/zhaoxuelu.png)

![](/images/5rating_dramas.gif)


## 🛡 Disclaimer

This project is intended **for educational and research purposes only**.  
The code is provided "as is" without any guarantees or warranties.

**Important:**  
- This repository does **not** include or distribute any data scraped from third-party websites.  
- Users are responsible for complying with the terms of service and robots.txt policies of any websites they scrape.  
- The author does not encourage or endorse any unlawful or unethical use of this code.

By using this code, you agree to use it responsibly and ethically.
