import pytest
import requests

from pipeline import config, healer
from pipeline.llm import ask_ollama


def ollama_is_up() -> bool:
    try:
        requests.get(config.ollama_url().replace("/api/generate", "/api/tags"), timeout=3).raise_for_status()
        return True
    except requests.RequestException:
        return False


pytestmark = [pytest.mark.live_llm, pytest.mark.skipif(not ollama_is_up(), reason="Ollama is not reachable")]


def test_real_llama_maps_renamed_columns(tmp_path):
    raw = tmp_path / "renamed.csv"
    raw.write_text("full_name,years,town\nAsha,30,Delhi\nRavi,41,Pune\n", encoding="utf-8")
    result = healer.heal_file(raw, tmp_path / "out.csv", ask_ollama)
    assert result.success, result.error_message
    assert result.method == "llm"
    assert result.mapping == {"name": "full_name", "age": "years", "city": "town"}
