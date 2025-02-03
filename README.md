# Real-Time-Customer-Data-Pipeline
## Overview  
This project is a real-time streaming data pipeline designed to generate and process synthetic customer data every 2 minutes. It leverages Apache NiFi for data integration, Snowflake for data warehousing, and Snowflake Streams for **Change Data Capture (CDC)** and historical tracking.  
## Tech Stack  
- **Python (Faker Library)** – Generates synthetic customer data every 2 minutes  
- **Apache NiFi** – Captures and loads new data into AWS S3 dynamically  
- **AWS S3** – Stores raw customer data files  
- **SnowPipe & Snowflake** – Loads data from S3 into a staging table in Snowflake  
- **Snowflake Merge & Tasks** – Automates merging of new/updated data into the customers table every 2 minutes  
- **Snowflake Streams & Views** – Tracks inserts, updates, and deletes in the staging table via a stream-based view  
- **Customers Table (SCD Type 1)** – Maintains the latest version of customer records  
- **Customers History Table (SCD Type 2)** – Uses the stream-based view to track historical changes  
## Workflow  
1. **Data Generation:**  
  - A Python script generates synthetic customer data every 2 minutes using the Faker library.  
2. **Data Ingestion:**  
  - Apache NiFi fetches new customer records and loads them into an AWS S3 bucket.  
  - NiFi ensures real-time ingestion whenever new data is available at the source.  
3. **Data Loading into Snowflake:**  
  - SnowPipe detects new data in S3 and loads it into a raw staging table in Snowflake.  
4. **Automated Data Processing:**  
  - A **merge function** updates the customers table with new or modified data from the staging table.  
  - This merge is scheduled using **Snowflake Tasks** to run every 2 minutes.  
  - The customers table follows **SCD Type 1 (SCD1)**, meaning it always reflects the latest customer data without keeping history.  
5. **Change Data Capture (CDC) & History Tracking:**  
  - **Snowflake Streams** enable **Change Data Capture (CDC)** by tracking inserts, updates, and deletes in the staging table.  
  - A **view is created from the stream** to structure the CDC data.  
  - Using this view, a **merge function updates the Customers History table**, implementing **SCD Type 2 (SCD2)** to maintain historical changes.  
