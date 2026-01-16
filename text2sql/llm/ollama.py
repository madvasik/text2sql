from typing import Optional
import time

import ollama

from .base import BaseLLMProvider


class OllamaProvider(BaseLLMProvider):
    """
    LLM-провайдер для локального Ollama.

    Работает через HTTP API (обычно http://localhost:11434).
    Не требует API-ключей.
    """

    def __init__(
        self,
        model: str = "qwen2.5:14b",
        timeout: float = 60.0,
        max_retries: int = 1,
    ):
        """
        Args:
            model: модель Ollama по умолчанию
            timeout: таймаут запроса (сек)
            max_retries: количество повторов при ошибке
        """
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries

        # Проверяем, что Ollama доступен сразу
        try:
            ollama.list()
        except Exception as e:
            raise RuntimeError(
                "Ollama is not available. "
                "Make sure Ollama is running (ollama serve)."
            ) from e

    def chat(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
    ) -> str:
        """
        Выполняет chat-запрос к Ollama и возвращает текст ответа модели.
        """
        last_error: Optional[Exception] = None
        model_name = model or self.model

        for attempt in range(self.max_retries + 1):
            try:
                response = ollama.chat(
                    model=model_name,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    options={
                        "temperature": 0.0,
                    },
                )

                content = response.get("message", {}).get("content")
                if not content:
                    raise RuntimeError("Empty response from Ollama model")

                return content.strip()

            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(0.5)
                    continue
                break

        raise RuntimeError(
            f"Ollama chat failed after {self.max_retries + 1} attempts: {last_error}"
        )
