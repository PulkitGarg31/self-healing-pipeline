import pytest

from pipeline import llm


def test_prompt_lists_columns_and_samples():
    prompt = llm.build_prompt(["full_name", "years"], [["Asha", "30"]])
    assert '["full_name", "years"]' in prompt
    assert '[["Asha", "30"]]' in prompt
    assert '"mapping"' in prompt


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


def test_ask_ollama_sends_a_json_request(monkeypatch):
    sent = {}

    def fake_post(url, json, timeout):
        sent.update(url=url, payload=json, timeout=timeout)
        return FakeResponse({"response": '{"mapping": {"name": "a", "age": "b", "city": "c"}}'})

    monkeypatch.setattr(llm.requests, "post", fake_post)
    monkeypatch.setenv("OLLAMA_URL", "http://ollama.test/api/generate")
    monkeypatch.setenv("OLLAMA_MODEL", "llama3.2")
    answer = llm.ask_ollama(["a", "b", "c"], [["x", "1", "y"]])
    assert answer == {"mapping": {"name": "a", "age": "b", "city": "c"}}
    assert sent["url"] == "http://ollama.test/api/generate"
    assert sent["payload"]["model"] == "llama3.2"
    assert sent["payload"]["format"] == "json"
    assert sent["payload"]["options"] == {"temperature": 0}
    assert sent["payload"]["stream"] is False
    assert sent["timeout"] == 60


def test_reply_that_is_not_json_keeps_the_raw_text(monkeypatch):
    monkeypatch.setattr(llm.requests, "post", lambda url, json, timeout: FakeResponse({"response": "Sorry, I cannot help"}))
    with pytest.raises(ValueError, match="Sorry, I cannot help"):
        llm.ask_ollama(["a"], [["x"]])
