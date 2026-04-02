import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl_pipeline import transform_data


class TestTransformData:
    def test_transform_returns_dataframe(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        assert isinstance(result, pd.DataFrame)

    def test_transform_has_expected_columns(self, sample_dataframe, transformed_columns):
        result = transform_data(sample_dataframe)
        for col in transformed_columns:
            assert col in result.columns, f"Missing expected column after transform: {col}"

    def test_launched_at_is_datetime(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        assert pd.api.types.is_datetime64_any_dtype(result["launched_at"])

    def test_deadline_at_is_datetime(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        assert pd.api.types.is_datetime64_any_dtype(result["deadline_at"])

    def test_duration_days_is_positive(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        assert (result["duration_days"] >= 0).all(), "Duration should be non-negative"

    def test_success_flag_binary(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        unique_flags = result["success_flag"].unique()
        assert all(flag in [0, 1] for flag in unique_flags), "success_flag should be 0 or 1"

    def test_pledged_usd_renamed(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        assert "pledged_usd" in result.columns
        assert "usd_pledged_real" not in result.columns

    def test_goal_usd_renamed(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        assert "goal_usd" in result.columns
        assert "usd_goal_real" not in result.columns

    def test_null_names_removed(self, sample_dataframe):
        original_count = len(sample_dataframe)
        result = transform_data(sample_dataframe)
        null_names = result["name"].isnull().sum()
        assert null_names == 0, "Null names should be removed"


class TestTransformEdgeCases:
    def test_transform_handles_empty_dataframe(self):
        empty_df = pd.DataFrame({
            "ID": pd.Series(dtype="int"),
            "name": pd.Series(dtype="str"),
            "category": pd.Series(dtype="str"),
            "main_category": pd.Series(dtype="str"),
            "currency": pd.Series(dtype="str"),
            "deadline": pd.Series(dtype="str"),
            "goal": pd.Series(dtype="float"),
            "launched": pd.Series(dtype="str"),
            "pledged": pd.Series(dtype="float"),
            "state": pd.Series(dtype="str"),
            "backers": pd.Series(dtype="int"),
            "country": pd.Series(dtype="str"),
            "usd_pledged_real": pd.Series(dtype="float"),
            "usd_goal_real": pd.Series(dtype="float"),
        })
        result = transform_data(empty_df)
        assert len(result) == 0

    def test_success_states(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        successful = result[result["state"] == "successful"]
        assert (successful["success_flag"] == 1).all(), "Successful campaigns should have success_flag=1"

        failed = result[result["state"] == "failed"]
        assert (failed["success_flag"] == 0).all(), "Failed campaigns should have success_flag=0"
