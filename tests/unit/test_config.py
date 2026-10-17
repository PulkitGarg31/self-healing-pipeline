import pytest

from pipeline import config


def test_expected_columns():
    assert config.EXPECTED_COLUMNS == ["name", "age", "city"]


def test_data_dir_comes_from_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path))
    assert config.data_dir() == tmp_path
    assert config.incoming_dir() == tmp_path / "incoming"


def test_db_settings_read_environment(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "db1")
    monkeypatch.setenv("POSTGRES_USER", "user1")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret1")
    monkeypatch.setenv("PIPELINE_DB_HOST", "host1")
    monkeypatch.delenv("PIPELINE_DB_PORT", raising=False)
    assert config.db_settings() == {
        "host": "host1",
        "port": 5432,
        "dbname": "db1",
        "user": "user1",
        "password": "secret1",
    }


def test_db_settings_have_no_default_password(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "db1")
    monkeypatch.setenv("POSTGRES_USER", "user1")
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    with pytest.raises(KeyError, match="POSTGRES_PASSWORD"):
        config.db_settings()
