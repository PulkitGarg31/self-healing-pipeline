import json

import pytest

from pipeline import llm


def test_prompt_lists_each_column_with_its_values():
    prompt = llm.build_prompt(["full_name", "years"], [["Asha", "30"], ["Ravi", "41"]])
    assert '- "full_name": ["Asha", "Ravi"]' in prompt
    assert '- "years": ["30", "41"]' in prompt
    assert '"mapping"' in prompt


def test_prompt_pads_a_short_row():
    prompt = llm.build_prompt(["full_name", "years"], [["Asha"]])
    assert '- "years": [""]' in prompt


class FakeResponse:
    def __init__(self, body, status_code=200, text=None):
        self.body = body
        self.status_code = status_code
        self.ok = status_code < 400
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        if isinstance(self.body, Exception):
            raise self.body
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


def test_http_error_keeps_ollamas_message(monkeypatch):
    reply = FakeResponse({"error": "model 'llama9' not found"}, status_code=404)
    monkeypatch.setattr(llm.requests, "post", lambda url, json, timeout: reply)
    with pytest.raises(RuntimeError) as excinfo:
        llm.ask_ollama(["a"], [["x"]])
    assert "HTTP 404" in str(excinfo.value)
    assert "llama9" in str(excinfo.value)


def test_reply_without_a_response_field(monkeypatch):
    monkeypatch.setattr(llm.requests, "post", lambda url, json, timeout: FakeResponse({"message": "hi"}))
    with pytest.raises(RuntimeError, match="no 'response' field"):
        llm.ask_ollama(["a"], [["x"]])


def test_reply_body_that_is_not_json(monkeypatch):
    reply = FakeResponse(ValueError("bad"), text="<html>")
    monkeypatch.setattr(llm.requests, "post", lambda url, json, timeout: reply)
    with pytest.raises(RuntimeError, match="not JSON"):
        llm.ask_ollama(["a"], [["x"]])


def test_raw_text_is_cut_at_300_characters(monkeypatch):
    monkeypatch.setattr(llm.requests, "post", lambda url, json, timeout: FakeResponse({"response": "x" * 400}))
    with pytest.raises(ValueError) as excinfo:
        llm.ask_ollama(["a"], [["x"]])
    assert "x" * 300 in str(excinfo.value)
    assert "x" * 301 not in str(excinfo.value)
