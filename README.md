# National Address Registry – ETL Pipeline

## Project Overview
This project implements an **automated ETL (Extract, Transform, Load) pipeline** for processing **National Address Registry (NAR)** data.

The pipeline reads address data from **multiple CSV files**, performs validation and transformation using **Python**, and loads the cleaned data into a **MySQL database** hosted on a college server.

This project was developed as part of a set of academic assignments to demonstrate **data engineering**, including database design, normalization, and automated data loading. My role involved end-to-end ownership of the National Address Registry (NAR) ETL pipeline, including data ingestion, transformation, database design, and loading.

---

## Key Objectives
- Automate loading of large address datasets
- Handle data split across multiple CSV files
- Apply data quality checks
- Store data in a **normalized (3NF) database schema**
- Connect Python to MySQL using `pyodbc`

---

## Technologies Used
- Python  
- pyodbc  
- MySQL  
- CSV datasets  
- Database normalization (3NF)  
- ER / schema design using 'Mermaid' (HTML + MJS)

---

## ETL Pipeline Description

### Extract
- Reads **26 CSV files** containing address data
- Files are processed sequentially to handle large data size

### Transform
- Basic validation checks (null values, formats)
- Data prepared to match the database schema
- Transformation logic handled in Python

### Load (Staging and Normalization)
- During the loading process, all incoming data was initially stored as VARCHAR in a staging table.  
- The data was then split into normalized tables based on the 3NF design, and appropriate data types were applied during the final load.  
- The staging table also included metadata columns such as the load timestamp and source CSV file name to support traceability and data auditing.

---

## Database Design
- Database designed and normalized to **Third Normal Form (3NF)**
- Separate tables created to reduce redundancy

---

## Data Handling Note
Raw CSV files are **not included** in this repository due to GitHub file size limits.

Only **code, schema, and documentation** are version-controlled.

