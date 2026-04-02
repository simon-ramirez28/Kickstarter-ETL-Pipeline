# 💰 Kickstarter Campaign Success Analysis ETL

This repository contains an ETL (Extract, Transform, Load) pipeline built to prepare a dataset of Kickstarter project records for analysis and loading into a data warehouse or downstream analytics store. The original dataset is available on Kaggle: https://www.kaggle.com/datasets/kemical/kickstarter-projects

## 🛠️ Technology Stack

- **Language:** Python 3.x
- **ETL Libraries:** Pandas (for data manipulation)
- **Data Warehouse:** SQLite3 (standard Python module, used as a lightweight DWH)
- **Logging:** Python logging module (for traceability and error handling)
- **Modeling:** Dimensional Modeling (Star Schema)
- **Testing:** pytest with coverage reporting
- **Containerization:** Docker & Docker Compose

## 🏗️ Data Pipeline Explanation (ETL)

The pipeline is executed via `src/etl_pipeline.py` and consists of three main phases:

### 1. Extraction (E)

**Goal:** To safely and efficiently read the raw data from the local storage.
- **Data Ingestion:** The script reads the raw ks-projects-201801.csv file from the `/data/raw` directory, and give us a raw Pandas DataFrame (kickstarter_df).
- **Error Handling:** Implements try-except blocks to catch file not found errors and logging to record the start and successful completion of the extraction, Console logs and file logs (`logs/etl_pipeline.log`) are included.

### 2. Transformation (T)

**Goal:** Clean the data, derive necessary metrics, and prepare the structure for the dimensional model.
- **Date Conversion:** Converts deadline and launched columns from object/string types to proper datetime objects. Enables time-based calculations and accurate dimensional modeling.
- **Duration Calculation:** Calculates the total campaign length in days, stored in the new column duration_days. Creates a key performance indicator (KPI) for analysis.
- **Monetary Unification:** Renames and standardizes the currency columns (`usd pledged` real and `usd_goal_real`) to pledged_usd and goal_usd. Ensures all monetary analysis uses consistent, USD-converted values.
- **Success Flag Creation:** Creates a binary column success_flag (1 for 'successful', 0 for all other states like 'failed', 'canceled', etc.). Simplifies analytical queries and machine learning feature engineering.
- **NULL Constraint Check:** Removes rows where the critical field name is null. Crucial Fix: Prevents the NOT NULL constraint failed error during the Load phase.
- **Column Selection:** Filters the DataFrame to include only the columns necessary for the Fact and Dimension tables. Prepares the data for the final loading structure.

### 3. Loading (L)

**Goal:** To map the transformed data into a Star Schema and load it into the SQLite Data Warehouse (`kickstarter_warehouse.db`).

| Table | Type | Purpose & Mapping |
|-------|------|-------------------|
| **Dim_State** | Dimension | Stores unique campaign statuses and the binary is_successful flag. **_Mapping:_** state column is mapped to a unique state_key. |
| **Dim_Category** | Dimension | Stores unique combinations of main_category and category. **_Mapping:_** Both columns are used to derive a unique category_key. |
| **Dim_Date** | Dimension | Stores every unique launch date and its temporal attributes (year, month, day_of_week, is_weekend). **_Mapping:_** The launched_at datetime is mapped to a numerical date_key (YYYYMMDD). |
| **Fact_Campaigns** | Fact | Stores the performance metrics (pledged_usd, goal_usd, backers, duration_days). **_Mapping:_** It receives the Foreign Keys (state_key, category_key, launched_date_key) to link to the dimensional data. |

## 📁 Project Structure

```
Kickstarter-ETL-Pipeline/
├── Dockerfile              # ETL container image
├── Dockerfile.test         # Test container image
├── docker-compose.yml      # Container orchestration
├── Makefile               # Development commands
├── requirements.txt        # Production dependencies
├── requirements-dev.txt   # Development dependencies
├── src/
│   └── etl_pipeline.py    # Main ETL script
├── sql/
│   └── create_tables.sql  # Star Schema DDL
├── data/
│   ├── raw/              # Raw CSV files
│   └── kickstarter_warehouse.db  # SQLite DWH
├── logs/
│   └── etl_pipeline.log   # Pipeline logs
└── tests/
    ├── conftest.py       # pytest fixtures
    ├── test_extract.py   # Extraction tests
    ├── test_transform.py # Transformation tests
    ├── test_integration.py # E2E tests
    └── fixtures/
        └── ks-projects-sample.csv # Sample dataset
```

## 🚀 Getting Started

### 1. Prerequisites

- Python 3.x installed
- Docker & Docker Compose (optional, for containerized execution)

### 2. Installation

```bash
# Clone the repository
git clone <YOUR_REPO_URL>
cd Kickstarter-ETL-Pipeline

# Install production dependencies
pip install -r requirements.txt

# Install development dependencies (includes pytest)
pip install -r requirements-dev.txt
```

### 3. Data Setup

- Download the `ks-projects-201801.csv` file from the [Kaggle dataset link](https://www.kaggle.com/datasets/kemical/kickstarter-projects).
- Place the downloaded file into the `data/raw/` directory.

### 4. Execution

**Option A: Direct Python execution**
```bash
python src/etl_pipeline.py
```

**Option B: Using Make**
```bash
make install    # Install dependencies
make test       # Run tests
make docker-run # Run ETL in Docker
```

**Option C: Docker Compose**
```bash
docker-compose build    # Build images
docker-compose up etl  # Run ETL pipeline
```

Upon successful completion, the data warehouse file `data/kickstarter_warehouse.db` will be generated, containing the fully modeled Star Schema.

## 🧪 Testing

Run the test suite with coverage reporting:

```bash
# Run all tests with verbose output
pytest tests/ -v

# Run tests with coverage report
pytest tests/ -v --cov=src --cov-report=term-missing

# Run tests in Docker
docker-compose run etl-test
```

**Test Coverage:**
- 13 tests covering extraction, transformation, and integration
- Fixtures with sample dataset (50 campaigns)
- Validation of data types, null constraints, and database schema

## 🐳 Docker Support

### Building Images

```bash
# Build ETL image
docker-compose build etl

# Build test image
docker-compose build etl-test
```

### Running Containers

```bash
# Run ETL pipeline
docker-compose up etl

# Run tests
docker-compose up etl-test

# Run in background
docker-compose up -d etl

# View logs
docker-compose logs -f etl
```

### Quick Commands (make)

| Command | Description |
|---------|-------------|
| `make install` | Install dependencies |
| `make test` | Run tests locally |
| `make test-cov` | Run tests with coverage |
| `make docker-build` | Build Docker images |
| `make docker-run` | Run ETL in Docker |
| `make docker-test` | Run tests in Docker |
| `make clean` | Remove generated files |

## 📊 Validation & Analysis

Use a SQLite client (like DB Browser for SQLite or VS Code SQLite extension) to open the `.db` file and run analytical queries against the dimensional model.

Example queries:

```sql
-- Success rate by category
SELECT c.main_category_name, 
       COUNT(*) as total,
       SUM(CASE WHEN s.is_successful = 1 THEN 1 ELSE 0 END) as successful,
       ROUND(AVG(f.pledged_usd), 2) as avg_pledged
FROM Fact_Campaigns f
JOIN Dim_State s ON f.state_key = s.state_key
JOIN Dim_Category c ON f.category_key = c.category_key
GROUP BY c.main_category_name
ORDER BY successful DESC;
```

---

> **Note:** Check the log file (`logs/etl_pipeline.log`) to see how the pipeline executes and for troubleshooting.
