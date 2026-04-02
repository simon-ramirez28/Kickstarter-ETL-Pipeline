import pytest
import pandas as pd
import sqlite3
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl_pipeline import extract_data, transform_data


SAMPLE_CSV = "data/raw/ks-projects-sample.csv"
TEST_DB = "data/test_warehouse.db"


class TestIntegration:
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        yield
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)

    def test_etl_pipeline_runs_without_error(self, sample_data_path):
        df = extract_data(sample_data_path)
        assert df is not None
        transformed = transform_data(df)
        assert transformed is not None
        assert len(transformed) > 0

    def test_transformed_data_has_valid_types(self, sample_data_path):
        df = extract_data(sample_data_path)
        transformed = transform_data(df)
        assert pd.api.types.is_datetime64_any_dtype(transformed["launched_at"])
        assert pd.api.types.is_datetime64_any_dtype(transformed["deadline_at"])
        assert transformed["duration_days"].dtype in ["float64", "int64"]
        assert transformed["success_flag"].dtype in ["int64", "int32"]

    def test_success_rate_calculation(self, sample_data_path):
        df = extract_data(sample_data_path)
        transformed = transform_data(df)
        total = len(transformed)
        successful = len(transformed[transformed["success_flag"] == 1])
        success_rate = successful / total if total > 0 else 0
        assert 0 <= success_rate <= 1

    def test_no_null_critical_fields(self, sample_data_path):
        df = extract_data(sample_data_path)
        transformed = transform_data(df)
        critical = ["ID", "name", "state", "main_category"]
        for col in critical:
            assert transformed[col].isnull().sum() == 0


class TestDatabaseSchema:
    @pytest.fixture(autouse=True)
    def setup_db(self):
        self.db_path = TEST_DB
        yield
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_dim_state_created(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Dim_State (
                state_key INTEGER PRIMARY KEY AUTOINCREMENT,
                state_name TEXT NOT NULL UNIQUE,
                is_successful INTEGER NOT NULL
            )
        """)
        conn.commit()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        assert "Dim_State" in tables

    def test_dim_category_created(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Dim_Category (
                category_key INTEGER PRIMARY KEY AUTOINCREMENT,
                main_category_name TEXT NOT NULL,
                sub_category_name TEXT NOT NULL,
                UNIQUE(main_category_name, sub_category_name)
            )
        """)
        conn.commit()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        assert "Dim_Category" in tables

    def test_dim_date_created(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Dim_Date (
                date_key INTEGER PRIMARY KEY,
                full_date TEXT NOT NULL UNIQUE,
                year INTEGER NOT NULL,
                quarter INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                day_of_week TEXT NOT NULL,
                is_weekend INTEGER NOT NULL
            )
        """)
        conn.commit()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        assert "Dim_Date" in tables

    def test_fact_campaigns_created(self):
        conn = sqlite3.connect(self.db_path)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS Dim_State (
                state_key INTEGER PRIMARY KEY AUTOINCREMENT,
                state_name TEXT NOT NULL UNIQUE,
                is_successful INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS Fact_Campaigns (
                campaign_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                backers INTEGER,
                pledged_usd REAL,
                goal_usd REAL,
                duration_days REAL,
                state_key INTEGER,
                category_key INTEGER,
                launched_date_key INTEGER
            );
        """)
        conn.commit()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        assert "Fact_Campaigns" in tables
