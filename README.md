# SQL-DataWarehouse-Medallion-Architecture-Project
Modern Data Architecture Project | Medallion Architecture (Bronze, Silver, Gold) using MS SQL Server &amp; ETL Pipelines

## 🏗️ Data Architecture

The architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

![Architecture](Documentation/2.Architecture.PNG)

