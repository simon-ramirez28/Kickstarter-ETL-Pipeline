import pytest
import pandas as pd
import os
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestExtractData:
    def test_file_exists(self, sample_data_path):
        assert sample_data_path.exists(), f"Sample data file not found: {sample_data_path}"

    def test_extract_returns_dataframe(self, sample_data_path):
        df = pd.read_csv(sample_data_path)
        assert isinstance(df, pd.DataFrame)

    def test_extract_has_rows(self, sample_data_path):
        df = pd.read_csv(sample_data_path)
        assert len(df) > 0, "DataFrame should have at least one row"

    def test_extract_has_expected_columns(self, sample_data_path, raw_columns):
        df = pd.read_csv(sample_data_path)
        for col in raw_columns:
            assert col in df.columns, f"Missing expected column: {col}"

    def test_extract_no_duplicate_ids(self, sample_data_path):
        df = pd.read_csv(sample_data_path)
        assert df["ID"].is_unique, "ID column should have unique values"

    def test_extract_required_columns_not_null(self, sample_data_path):
        df = pd.read_csv(sample_data_path)
        required = ["ID", "name", "state", "launched", "deadline"]
        for col in required:
            null_count = df[col].isnull().sum()
            assert null_count == 0, f"Column {col} should not have null values"


class TestExtractEdgeCases:
    def test_extract_missing_file_returns_none(self):
        from src.etl_pipeline import extract_data
        result = extract_data("nonexistent_file.csv")
        assert result is None

    def test_extract_empty_dataframe_shape(self, sample_dataframe):
        assert sample_dataframe.shape[0] > 0
        assert sample_dataframe.shape[1] > 0
