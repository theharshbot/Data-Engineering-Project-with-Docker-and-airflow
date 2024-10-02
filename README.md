# Data-Engineering-Project-with-Docker-and-airflow
This is a DE project which parses wikepedia to obtain information regarding the EPL football clubs and transforms the data, pushes the data to PostgresWebserver


Project Description
This project is an automated data pipeline that extracts, transforms, and loads (ETL) Premier League football club data from Wikipedia into a PostgreSQL database. The pipeline is built using Apache Airflow and automates the entire process of scraping data, cleaning and transforming it, and inserting it into a relational database for further analysis.

Purpose
The goal of this project is to provide an end-to-end solution for retrieving and managing football club data for analytics or reporting purposes. The pipeline is designed to demonstrate how web scraping, data transformation, and database interactions can be automated using Airflow, making it highly scalable and maintainable.

Main Features
Web Scraping: Uses requests and BeautifulSoup to scrape club information (e.g., club name, location, seasons, spells, etc.) from Wikipedia.
Data Transformation: Cleans and processes the data using Python and Pandas to ensure consistency, accuracy, and completeness.
Database Integration: Uses PostgreSQL as the target database to store the transformed data for further use in analytics or reporting.
Automation with Airflow: The ETL pipeline is orchestrated using Apache Airflow, which schedules, monitors, and automates the workflow.
Error Handling: Incorporates error handling during web scraping and database insertion steps to ensure data integrity and robustness.
Scalability: Designed to easily extend with more data sources or additional features like geolocation or advanced analytics.
Technologies Used
Apache Airflow: For orchestration and scheduling of the ETL pipeline.
Python: For web scraping, data transformation, and database interactions.
BeautifulSoup: For parsing the HTML content from the Wikipedia page.
PostgreSQL: For storing the transformed data.
Docker: For containerizing the services, including Airflow and PostgreSQL, making it easy to set up and deploy the project.
How It Works
Extract: The pipeline scrapes the Premier League clubs' data from the Wikipedia page.
Transform: The scraped data is cleaned and formatted for consistency using Pandas.
Load: The transformed data is inserted into a PostgreSQL table.
Orchestration: The entire process is automated using Apache Airflow, which ensures the pipeline runs on schedule and handles retries in case of failure.
