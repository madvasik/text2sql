import os
from typing import Optional

from .base import BaseLLMProvider
from .ollama import OllamaProvider

# Mistral делаем опциональным, чтобы проект работал без него
try:
    from .mistral import MistralProvider
except ImportError:
    MistralProvider = None


def get_provider() -> BaseLLMProvider:
    """
    Фабрика LLM-провайдера.

    Выбор осуществляется через переменную окружения:
        LLM_PROVIDER=ollama | mistral

    По умолчанию используется Ollama.
    """
    provider_name = os.getenv("LLM_PROVIDER", "ollama").lower()

    if provider_name == "mistral":
        if MistralProvider is None:
            raise RuntimeError(
                "Mistral provider is not available. "
                "Install mistralai package or switch LLM_PROVIDER to 'ollama'."
            )
        return MistralProvider()

    if provider_name == "ollama":
        return OllamaProvider()

    raise ValueError(
        f"Unknown LLM_PROVIDER='{provider_name}'. "
        "Allowed values: 'ollama', 'mistral'."
    )


def get_model_name(default: Optional[str] = None) -> Optional[str]:
    """
    Возвращает имя модели из переменной окружения LLM_MODEL,
    либо default, если не задано.

    Пример:
        LLM_MODEL=qwen2.5:14b
    """
    return os.getenv("LLM_MODEL", default)
