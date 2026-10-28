"""Tests for the OpenRouter response handling."""

from types import SimpleNamespace

import pytest

from ai_suggester.llm_backend import LLMResponseError, extract_content


def _resp(content, has_choices=True, error=None):
    choices = [SimpleNamespace(message=SimpleNamespace(content=content))] if has_choices else None
    return SimpleNamespace(choices=choices, error=error)


def test_returns_message_content():
    assert extract_content(_resp('[{"name": "a"}]')) == '[{"name": "a"}]'


def test_no_choices_raises_with_provider_error():
    with pytest.raises(LLMResponseError, match="rate limited"):
        extract_content(_resp(None, has_choices=False, error="rate limited"))


def test_no_choices_without_detail_still_raises():
    with pytest.raises(LLMResponseError, match="no choices"):
        extract_content(SimpleNamespace(choices=[]))


def test_empty_content_raises():
    with pytest.raises(LLMResponseError, match="empty"):
        extract_content(_resp("   "))
