from abc import ABC, abstractmethod
from typing import Optional


class BaseLLMProvider(ABC):
    """
    Базовый интерфейс для LLM-провайдеров.

    Любая реализация (Ollama, Mistral, OpenAI и т.д.)
    должна уметь выполнить chat-запрос и вернуть
    ТОЛЬКО текст ответа модели.
    """

    @abstractmethod
    def chat(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
    ) -> str:
        """
        Выполняет chat completion запрос.

        Args:
            system: System prompt
            user: User prompt
            model: Имя модели (если None — используется дефолтная)

        Returns:
            str: сырой текст ответа модели (без постобработки)

        Raises:
            RuntimeError: если запрос к LLM не удался
        """
        raise NotImplementedError("LLM provider must implement chat()")
