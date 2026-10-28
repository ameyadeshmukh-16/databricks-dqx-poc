"""
Pluggable LLM backend — abstracts Claude (Anthropic), Azure OpenAI, and
OpenRouter behind a single interface so callers never depend on a specific SDK.

Select the backend via:
  get_backend("claude")        — Anthropic API
  get_backend("azure_openai")  — Azure OpenAI API
  get_backend("openrouter")    — OpenRouter API (free/paid models)
  get_backend()                — reads LLM_BACKEND env var (default: "claude")
"""

from __future__ import annotations

import os
import time
from abc import ABC, abstractmethod

from src.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_BACKEND = "claude"


class LLMBackend(ABC):
    """Abstract LLM backend.  Subclasses implement generate()."""

    # The model that actually answered the last call (providers can route "auto"/"free"
    # aliases to different models, so this is recorded as provenance for each suggestion).
    last_model: str | None = None

    @abstractmethod
    def generate(self, prompt: str, system: str = "") -> str:
        """Send prompt to the LLM and return the response text."""


class ClaudeBackend(LLMBackend):
    """Anthropic Claude API backend."""

    def __init__(self) -> None:
        try:
            import anthropic  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "anthropic package not installed. Run: pip install anthropic>=0.25.0"
            ) from exc

        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise EnvironmentError(
                "ANTHROPIC_API_KEY is not set. Add it to your .env or cluster env vars."
            )

        self._api_key = api_key
        self._model = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")

        logger.info("ClaudeBackend initialised — model: %s", self._model)

    def generate(self, prompt: str, system: str = "") -> str:
        import anthropic

        client = anthropic.Anthropic(api_key=self._api_key)

        message = client.messages.create(
            model=self._model,
            max_tokens=4096,
            system=system or "You are a data quality expert.",
            messages=[{"role": "user", "content": prompt}],
        )

        self.last_model = getattr(message, "model", None) or self._model
        return message.content[0].text


class AzureOpenAIBackend(LLMBackend):
    """Azure OpenAI API backend."""

    def __init__(self) -> None:
        try:
            import openai  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "openai package not installed. Run: pip install openai>=1.0.0"
            ) from exc

        self._api_key = os.environ.get("AZURE_OPENAI_API_KEY", "")
        self._endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
        self._deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "")
        self._api_version = os.environ.get(
            "AZURE_OPENAI_API_VERSION",
            "2024-02-01"
        )

        missing = [
            k for k, v in {
                "AZURE_OPENAI_API_KEY": self._api_key,
                "AZURE_OPENAI_ENDPOINT": self._endpoint,
                "AZURE_OPENAI_DEPLOYMENT": self._deployment,
            }.items() if not v
        ]

        if missing:
            raise EnvironmentError(
                f"Missing Azure OpenAI env vars: {missing}. "
                "Add them to your .env or cluster env vars."
            )

        logger.info(
            "AzureOpenAIBackend initialised — deployment: %s",
            self._deployment
        )

    def generate(self, prompt: str, system: str = "") -> str:
        from openai import AzureOpenAI

        client = AzureOpenAI(
            api_key=self._api_key,
            azure_endpoint=self._endpoint,
            api_version=self._api_version,
        )

        response = client.chat.completions.create(
            model=self._deployment,
            messages=[
                {
                    "role": "system",
                    "content": system or "You are a data quality expert.",
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=4096,
            temperature=0.2,
        )

        self.last_model = self._deployment
        return response.choices[0].message.content


# ==========================================================
# NEW: OpenRouter Backend
# ==========================================================
def parse_model_list(value: str | None) -> list[str]:
    """'a, b ,c' -> ['a', 'b', 'c'] (empty entries dropped); never returns an empty list."""
    models = [m.strip() for m in (value or "").split(",") if m.strip()]
    return models or ["openrouter/auto"]


class LLMResponseError(RuntimeError):
    """The provider answered, but without usable message content."""


def extract_content(response) -> str:
    """Return the message text from a chat-completions response or raise LLMResponseError."""
    choices = getattr(response, "choices", None)
    if not choices:
        detail = getattr(response, "error", None) or "no choices in response"
        raise LLMResponseError(f"Provider returned no choices: {detail}")
    content = getattr(getattr(choices[0], "message", None), "content", None)
    if not content or not str(content).strip():
        raise LLMResponseError("Provider returned an empty message")
    return content


class OpenRouterBackend(LLMBackend):
    """OpenRouter API backend (OpenAI-compatible)."""

    def __init__(self) -> None:
        try:
            import openai  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "openai package not installed. Run: pip install openai>=1.0.0"
            ) from exc

        self._api_key = os.environ.get("OPENROUTER_API_KEY", "")
        self._base_url = os.environ.get(
            "OPENAI_BASE_URL",
            "https://openrouter.ai/api/v1"
        )

        # One model, or an ordered comma-separated list: if the first fails the next is tried.
        # (The 'openrouter/free' router can return empty answers; pinned models were more reliable.)
        self._models = parse_model_list(os.environ.get("OPENROUTER_MODEL", "openrouter/auto"))
        self._model = self._models[0]

        if not self._api_key:
            raise EnvironmentError(
                "OPENROUTER_API_KEY is not set. "
                "Add it to your .env or cluster env vars."
            )

        logger.info(
            "OpenRouterBackend initialised — model: %s",
            self._model
        )

    def generate(self, prompt: str, system: str = "") -> str:
        from openai import OpenAI

        client = OpenAI(
            api_key=self._api_key,
            base_url=self._base_url,
            timeout=60.0,
        )

        # The free routed model sometimes answers 200 with no choices (upstream
        # error / rate limit). Retry once, then fail with the provider's message.
        last_error = None
        attempts = 2 if len(self._models) == 1 else 1      # with a fallback list, move on instead of retrying
        for model in self._models:
            for attempt in range(1, attempts + 1):
                try:
                    response = client.chat.completions.create(
                        model=model,
                        messages=[
                            {
                                "role": "system",
                                "content": system or "You are a data quality expert.",
                            },
                            {"role": "user", "content": prompt},
                        ],
                        max_tokens=4096,
                        temperature=0.2,
                    )
                    self.last_model = getattr(response, "model", None) or model
                    return extract_content(response)
                except Exception as exc:                   # noqa: BLE001 - provider errors, empty answers, timeouts
                    last_error = exc
                    logger.warning("OpenRouter model %s attempt %d/%d failed: %s",
                                   model, attempt, attempts, str(exc).splitlines()[0][:160])
                    if attempt < attempts:
                        time.sleep(3)
        raise last_error


def describe_backend(llm: LLMBackend) -> str:
    """Provenance label stored with a suggestion, e.g. 'openrouter:meta-llama/llama-3.3-70b-instruct:free'."""
    provider = type(llm).__name__.replace("Backend", "").lower()
    return f"{provider}:{llm.last_model}" if getattr(llm, "last_model", None) else provider


def get_backend(name: str | None = None) -> LLMBackend:
    """
    Return an LLMBackend instance.

    name:
      "claude"
      "azure_openai"
      "openrouter"
      None (reads LLM_BACKEND env var)
    """

    resolved = (
        name or os.environ.get("LLM_BACKEND", _DEFAULT_BACKEND)
    ).lower().strip()

    logger.info("LLM backend resolved to: %s", resolved)

    if resolved == "claude":
        return ClaudeBackend()

    if resolved in (
        "azure_openai",
        "azure-openai",
        "azureopenai",
    ):
        return AzureOpenAIBackend()

    # ==========================================================
    # NEW: OpenRouter resolution
    # ==========================================================
    if resolved == "openrouter":
        return OpenRouterBackend()

    raise ValueError(
        f"Unknown LLM backend: '{resolved}'. "
        "Valid values: 'claude', 'azure_openai', 'openrouter'."
    )