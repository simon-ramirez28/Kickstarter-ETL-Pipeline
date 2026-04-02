import pytest
import pandas as pd
import os
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_DATA_PATH = FIXTURES_DIR / "ks-projects-sample.csv"


@pytest.fixture
def sample_data_path():
    return SAMPLE_DATA_PATH


@pytest.fixture
def sample_dataframe():
    return pd.read_csv(SAMPLE_DATA_PATH)


@pytest.fixture
def raw_columns():
    return [
        "ID", "name", "category", "main_category", "currency", "deadline",
        "goal", "launched", "pledged", "state", "backers",
        "country", "usd_pledged_real", "usd_goal_real"
    ]


@pytest.fixture
def transformed_columns():
    return [
        "ID", "name", "main_category", "category", "country", "backers",
        "pledged_usd", "goal_usd", "success_flag", "state",
        "launched_at", "deadline_at", "duration_days"
    ]


@pytest.fixture
def expected_states():
    return ["successful", "failed", "canceled", "suspended", "live", "undefined"]


@pytest.fixture
def expected_categories():
    return ["Film & Video", "Music", "Publishing", "Games", "Technology"]
