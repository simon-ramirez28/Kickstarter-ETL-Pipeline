# 💰 Kickstarter Campaign Success Analysis ETL

This repository contains an ETL (Extract, Transform, Load) pipeline built to prepare a dataset of Kickstarter project records for analysis and loading into a data warehouse or downstream analytics store. The original dataset is available on Kaggle: https://www.kaggle.com/datasets/kemical/kickstarter-projects

## 🛠️ Technology Stack

    Language: Python 3.x

    ETL Libraries: Pandas (for data manipulation)

    Data Warehouse: SQLite3 (standard Python module, used as a lightweight DWH)

    Logging: Python logging module (for traceability and error handling)

    Modeling: Dimensional Modeling (Star Schema)

## 🏗️ Data Pipeline Explanation (ETL)

The pipeline is executed via src/etl_pipeline.py and consists of three main phases:

### 1. Extraction (E)

**Goal:** To safely and efficiently read the raw data from the local storage.
**Step** | **Action** | **Output**
Data Ingestion | The script reads the raw ks-projects-201801.csv file from the /data/raw directory.	| A raw Pandas DataFrame (kickstarter_df).
Error Handling | Implements try-except blocks to catch file not found errors and logging to record the start and successful completion of the extraction. | Console logs and file logs (logs/etl_pipeline.log).

### 2. Transformation (T)

Goal: Clean the data, derive necessary metrics, and prepare the structure for the dimensional model.
Step	Transformation	Rationale/Modification
Date Conversion	Converts deadline and launched columns from object/string types to proper datetime objects.	Enables time-based calculations and accurate dimensional modeling.
Duration Calculation	Calculates the total campaign length in days, stored in the new column duration_days.	Creates a key performance indicator (KPI) for analysis.
Monetary Unification	Renames and standardizes the currency columns (usd pledged real and usd_goal_real) to pledged_usd and goal_usd.	Ensures all monetary analysis uses consistent, USD-converted values.
Success Flag Creation	Creates a binary column success_flag (1 for 'successful', 0 for all other states like 'failed', 'canceled', etc.).	Simplifies analytical queries and machine learning feature engineering.
NULL Constraint Check	Removes rows where the critical field name is null.	Crucial Fix: Prevents the NOT NULL constraint failed error during the Load phase.
Column Selection	Filters the DataFrame to include only the columns necessary for the Fact and Dimension tables.	Prepares the data for the final loading structure.

### 3. Loading (L)

Goal: To map the transformed data into a Star Schema and load it into the SQLite Data Warehouse (kickstarter_warehouse.db).
Table	Type	Purpose & Mapping
Dim_State	Dimension	Stores unique campaign statuses and the binary is_successful flag. Mapping: state column is mapped to a unique state_key.
Dim_Category	Dimension	Stores unique combinations of main_category and category. Mapping: Both columns are used to derive a unique category_key.
Dim_Date	Dimension	Stores every unique launch date and its temporal attributes (year, month, day_of_week, is_weekend). Mapping: The launched_at datetime is mapped to a numerical date_key (YYYYMMDD).
Fact_Campaigns	Fact	Stores the performance metrics (pledged_usd, goal_usd, backers, duration_days). Mapping: It receives the Foreign Keys (state_key, category_key, launched_date_key) to link to the dimensional data.

🚀 Getting Started

Follow these steps to replicate and run the pipeline:

1. Prerequisites

You need Python 3.x installed.
Bash

# Clone the repository (once uploaded to GitHub)
`git clone <YOUR_REPO_URL>
cd <YOUR_PROJECT_FOLDER>`

# Install dependencies (pandas, etc.)
`pip install -r requirements.txt`

2. Data Setup

    Download the ks-projects-201801.csv file from the Kaggle dataset link.

    Place the downloaded file into the data/raw/ directory.

3. Execution

Run the main ETL script from the project root:
Bash

`python src/etl_pipeline.py`

Upon successful completion, the data warehouse file, data/kickstarter_warehouse.db, will be generated, containing the fully modeled Star Schema.

4. Validation & Analysis

Use a SQLite client (like DB Browser for SQLite) to open the .db file and run analytical queries against the dimensional model.
---
> **Note:** Or Just simply check the log file to see how this pipeline works
