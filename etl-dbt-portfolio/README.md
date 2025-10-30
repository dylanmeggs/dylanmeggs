# ETL / Data Warehouse Portfolio Example

This is a **mock data warehouse** project built with dbt to demonstrate ETL pipelines and data modeling.  

### Features:
- DoorDash-style mock data: customers, drivers, orders, businesses  
- Staging, intermediate, and mart layers for dbt  
- Example SQL models that join and aggregate data  
- Designed to visualize **data flow from raw tables to BI-ready tables**

### How to run:
1. Install dbt.
2. Configure a target database (Postgres, Redshift, or Snowflake).
3. Load CSV data into raw tables.
4. Run `dbt run` to materialize models.