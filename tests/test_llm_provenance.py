"""Tests for recording which model produced a suggestion."""

import sys
from types import SimpleNamespace

from ai_suggester.llm_backend import (
    AzureOpenAIBackend, ClaudeBackend, LLMBackend, OpenRouterBackend, describe_backend,
)


class _Fake(LLMBackend):
    def generate(self, prompt, system=""):
        return ""


def test_describe_includes_the_model_once_it_is_known():
    llm = _Fake()
    assert describe_backend(llm) == "_fake"                         # nothing answered yet
    llm.last_model = "meta-llama/llama-3.3-70b-instruct:free"
    assert describe_backend(llm) == "_fake:meta-llama/llama-3.3-70b-instruct:free"


def test_provider_name_matches_the_backend_class():
    assert describe_backend(object.__new__(OpenRouterBackend)) == "openrouter"
    assert describe_backend(object.__new__(ClaudeBackend)) == "claude"
    assert describe_backend(object.__new__(AzureOpenAIBackend)) == "azureopenai"


def test_openrouter_records_the_model_that_actually_answered(monkeypatch):
    backend = object.__new__(OpenRouterBackend)
    backend._api_key, backend._base_url = "k", "http://x"
    backend._models = backend._model = None
    backend._models, backend._model = ["openrouter/free"], "openrouter/free"
    response = SimpleNamespace(model="some/served-model:free",
                               choices=[SimpleNamespace(message=SimpleNamespace(content="[]"))])

    class FakeClient:
        def __init__(self, **kw):
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=lambda **kw: response))

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeClient))   # package may not be installed locally
    assert backend.generate("p") == "[]"
    assert describe_backend(backend) == "openrouter:some/served-model:free"


def test_openrouter_falls_back_to_the_requested_model_name(monkeypatch):
    backend = object.__new__(OpenRouterBackend)
    backend._api_key, backend._base_url = "k", "http://x"
    backend._models = backend._model = None
    backend._models, backend._model = ["openrouter/free"], "openrouter/free"
    response = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="[]"))])

    class FakeClient:
        def __init__(self, **kw):
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=lambda **kw: response))

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeClient))   # package may not be installed locally
    backend.generate("p")
    assert backend.last_model == "openrouter/free"


# ---------------------------------------------------------------------------
# model list and fallback
# ---------------------------------------------------------------------------

import pytest

from ai_suggester.llm_backend import LLMResponseError, parse_model_list


def test_model_list_parsing():
    assert parse_model_list("a, b ,c") == ["a", "b", "c"]
    assert parse_model_list("single") == ["single"]
    assert parse_model_list("") == ["openrouter/auto"]
    assert parse_model_list(None) == ["openrouter/auto"]
    assert parse_model_list(" , ,") == ["openrouter/auto"]


def _backend(models):
    backend = object.__new__(OpenRouterBackend)
    backend._api_key, backend._base_url = "k", "http://x"
    backend._models, backend._model = list(models), models[0]
    return backend


def _client_for(monkeypatch, behaviour):
    """behaviour(model) -> response object or raises; records the order models were tried."""
    tried = []

    def create(**kw):
        tried.append(kw["model"])
        return behaviour(kw["model"])

    class FakeClient:
        def __init__(self, **kw):
            self.chat = SimpleNamespace(completions=SimpleNamespace(create=create))

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=FakeClient))
    monkeypatch.setattr("ai_suggester.llm_backend.time.sleep", lambda s: None)
    return tried


def _ok(model, text="[]"):
    return SimpleNamespace(model=model, choices=[SimpleNamespace(message=SimpleNamespace(content=text))])


def test_the_next_model_is_used_when_the_first_returns_nothing(monkeypatch):
    def behaviour(model):
        return SimpleNamespace(model=model, choices=None) if model == "bad" else _ok(model, "[1]")
    tried = _client_for(monkeypatch, behaviour)
    backend = _backend(["bad", "good"])
    assert backend.generate("p") == "[1]"
    assert tried == ["bad", "good"]                       # one attempt on the failing model, then on to the next
    assert describe_backend(backend) == "openrouter:good"  # provenance names the model that really answered


def test_the_next_model_is_used_when_the_first_raises(monkeypatch):
    def behaviour(model):
        if model == "boom":
            raise RuntimeError("429 rate limited")
        return _ok(model)
    tried = _client_for(monkeypatch, behaviour)
    assert _backend(["boom", "good"]).generate("p") == "[]"
    assert tried == ["boom", "good"]


def test_all_models_failing_raises_the_last_error(monkeypatch):
    tried = _client_for(monkeypatch, lambda m: SimpleNamespace(model=m, choices=None))
    with pytest.raises(LLMResponseError):
        _backend(["a", "b"]).generate("p")
    assert tried == ["a", "b"]


def test_a_single_model_is_retried_once(monkeypatch):
    calls = {"n": 0}

    def behaviour(model):
        calls["n"] += 1
        return SimpleNamespace(model=model, choices=None) if calls["n"] == 1 else _ok(model, "[2]")
    tried = _client_for(monkeypatch, behaviour)
    assert _backend(["only"]).generate("p") == "[2]"
    assert tried == ["only", "only"]
