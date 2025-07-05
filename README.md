# Databricks_dbt-flight-pipeline

---

## Key Features

- Initial & Incremental Load Handling  
  Parameterized notebooks designed to switch between initial load and incremental CDC processing dynamically.

- Change Data Capture (CDC) and Slowly Changing Dimensions (SCD Type 2)  
  Tracks and stores historical changes in dimension tables using metadata columns.

- Dynamic Notebooks  
  Notebooks adapt their logic based on runtime parameters (e.g., load type, table name).

- Parallelism with Multi-threading  
  Improved performance for dimension and fact loads via Python multi-threading.

- Gold Layer with Upsert Logic  
  Upsert operations using MERGE for fact/dimension tables to keep data consistent and accurate.

- DBT Integration  
  Final gold data modeled and queried using DBT for SQL-based transformation and testing.

---

## How to Run

1. Clone this repo or upload files to your Databricks workspace.
2. Upload datasets to `/dbfs/data/` or Unity Catalog.
3. Start with the `Setup` notebook to initialize widgets and configs.
4. Run Bronze → Silver → Dimension Builder → Fact Builder → DBT.

---

## Concepts Demonstrated

- Big Data Ingestion with Autoloader
- Lakehouse Design Pattern (Bronze/Silver/Gold)
- Parameterization & Dynamic Pipelines
- Upserts with Delta MERGE
- SCD Type 2 Implementation
- Modular, Scalable Notebooks
- DBT for post-Databricks modeling

---

## Screenshots

Screenshots of key notebooks, DLT pipeline, and DBT output can be found in the `images/` folder..

---

## Quick Start with .dbc File

This repository includes a `.dbc` (Databricks archive) file containing all project notebooks.  
You can import it directly into your Databricks workspace:

1. Go to **Workspace** in Databricks.
2. Click the **dropdown arrow** next to your user folder and select **Import**.
3. Upload the `.dbc` file from this repository.
4. The full notebook structure will be imported and ready to run.

This allows you to explore or demo the full project without setting up notebooks manually.


---

## Author

**Sanu4039**  
Reach out on GitHub for questions or improvements.
