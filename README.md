Boston 311 Cloud-Native Data Pipeline and Text-to-SQL LLM
This project showcases an end-to-end cloud-native data pipeline built using Google Cloud Platform (GCP) to automate data ingestion from the Boston 311 API. The pipeline leverages modern data technologies like MotherDuck (as the data warehouse), Apache Superset (for visualization), and a Text-to-SQL LLM (for natural language querying). The solution demonstrates how cloud-native tools and machine learning can streamline data workflows and enable efficient data analysis.

Project Features
1. Cloud-Native Data Pipeline
Data Source: Boston 311 API, which provides real-time and historical data about 311 service requests in Boston.
Automation: A robust ETL/ETLT pipeline built on GCP to automate data ingestion and processing.
Tools Used:
Google Cloud Functions: For serverless execution of ETL jobs.
Google Cloud Storage (GCS): For storing raw and transformed data.
2. Data Warehousing
MotherDuck:
A lightweight and fast data warehouse solution built for the cloud.
Stores clean, transformed datasets for downstream analysis.
Indexing and partitioning strategies were applied to optimize query performance.
3. Data Visualization
Apache Superset:
An open-source visualization tool connected to MotherDuck for creating interactive dashboards.
Provides actionable insights into service request trends, resolutions, and geographical distribution.
4. Text-to-SQL Natural Language Querying
Streamlit Application:
A user-friendly interface that allows non-technical users to query the data warehouse using natural language.
Text-to-SQL LLM:
Leverages a pre-trained language model to convert natural language questions into SQL queries.
Facilitates efficient data retrieval for ad-hoc analysis.
Technologies Used
Google Cloud Platform (GCP):
Cloud Functions
Cloud Storage
MotherDuck: Data warehouse for efficient querying.
Apache Superset: Visualization and dashboarding tool.
Streamlit: Front-end framework for deploying the Text-to-SQL application.
Python:
For ETL pipeline development.
For integrating the Text-to-SQL LLM.
APIs: Boston 311 API for data collection.
