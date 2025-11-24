# 💰 Kickstarter Campaign Success Analysis ETL

This repository contains an ETL (Extract, Transform, Load) pipeline built to prepare a dataset of Kickstarter project records for analysis and loading into a data warehouse or downstream analytics store. The original dataset is available on Kaggle: https://www.kaggle.com/datasets/kemical/kickstarter-projects

## Overview
- Ingests the raw Kickstarter projects CSV file from `data/raw/`.
- Cleans and transforms the raw data into an analysis-ready tabular form.
- Loads the cleaned data into `data/processed/` or a target data warehouse/database.

**Phases**

### Extraction:
- Source: the pipeline reads the raw CSV located in `data/raw/` (for example `ks-projects-201801.csv`).
- Purpose: capture a faithful snapshot of the source data and perform light validation so transformation can assume a predictable input shape.
- Typical steps:
	- Read the CSV with a robust CSV reader (handling different encodings and separators).
	- Validate the presence of required columns and expected types where feasible.
	- Log or surface any gross issues (corrupt rows, missing file, unreadable lines).
	- Persist a copy of the raw snapshot (optional) so the pipeline is auditable and repeatable.

### Transformation:
This is the most important phase — where raw records become clean, consistent, and useful for analysis.

- Goals:
	- Clean noisy, inconsistent, and malformed values.
	- Convert textual fields to the correct types (dates, numeric, booleans).
	- Derive new features useful for analysis and reporting.
	- Enforce data quality rules and make the dataset idempotent when re-run.

- Key operations commonly performed:
	- Parsing timestamps: convert `launched_at`, `deadline` and similar fields to timezone-aware datetimes and compute campaign duration (e.g., `duration_days = deadline - launched_at`).
	- Numeric normalization: convert `goal` to numeric and, if present, normalize currencies into a single reference currency (e.g., USD) using exchange rates or a consistent conversion approach.
	- Category normalization: standardize `category` and `subcategory` names/ids so aggregate queries are stable.
	- Handling nulls: define and apply rules for missing values (fill, infer, or mark explicitly).
	- Deduplication: remove or consolidate duplicate records using a deterministic key.
	- Creating derived fields: e.g., `successful` flag (goal reached), `pledged_ratio`, `launch_month`, `country_code`, `duration_days`, and other engineered metrics.
	- Data validation checks: row counts, required-column completeness thresholds, value-range checks (e.g., non-negative goals), and uniqueness of primary keys.
	- Logging and testability: emit transformation summaries (counts changed, rows removed) and fail-fast when critical quality gates are not met.

- Importance and practices:
	- Keep transformations deterministic and idempotent: running the step multiple times on the same raw input should produce the same output.
	- Break complex transformations into smaller, testable functions (for unit testing and maintenance).
	- Treat the Transformation step as the place to enforce business rules and provenance metadata (e.g., `transformed_at`, `source_file`, `source_row_id`).

### Load:
- Target: write the transformed dataset to `data/processed/` (CSV, Parquet) or load into a data warehouse or database table.
- Considerations:
	- Write mode: choose `overwrite` for full snapshots or `append`/`upsert` for incremental loads — implement idempotency where possible.
	- Partitioning: write data partitioned by a useful column (e.g., year/month of `launched_at`) if using Parquet or a data warehouse to improve query performance.
	- Schema enforcement: ensure the target schema is compatible with the transformed dataset (types, column names, nullability).
	- Post-load validation: confirm row counts, sample checks, and any referential integrity constraints.
	- Logging and observability: record when the load ran, how many rows were written, and any errors encountered.

**Notes & Usage**
- The main ETL orchestration is implemented in `src/etl_pipeline.py` (see the Log to see the FULL PROCESS).