"""Configurable LLM client with streaming SSE support."""

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv


class LLMClient:
    """A reusable chat-completion client for any OpenAI-compatible endpoint."""

    def __init__(self, url: str, env_var: str, default_model: str):
        self.url = url
        self.env_var = env_var
        self.default_model = default_model

    def _get_api_key(self) -> str:
        load_dotenv(Path(__file__).resolve().parent.parent / ".env")
        key = os.getenv(self.env_var)
        if not key:
            raise RuntimeError(f"{self.env_var} not set in .env")
        return key

    def chat(self, messages: list[dict], model: str | None = None, **kwargs) -> str:
        """Send a chat completion and return the full response text.

        Streams the response and collects the content chunks.
        """
        api_key = self._get_api_key()

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Accept": "text/event-stream",
        }

        payload = {
            "model": model or self.default_model,
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", 16384),
            "temperature": kwargs.get("temperature", 0.2),
            "top_p": kwargs.get("top_p", 0.9),
            "stream": True,
        }

        response = requests.post(
            self.url, headers=headers, json=payload, stream=True, timeout=120
        )
        response.raise_for_status()

        content_parts = []
        for line in response.iter_lines():
            if not line:
                continue
            decoded = line.decode("utf-8")
            if not decoded.startswith("data: "):
                continue
            data = decoded[6:]  # strip "data: "
            if data.strip() == "[DONE]":
                break
            try:
                chunk = json.loads(data)
                delta = chunk.get("choices", [{}])[0].get("delta", {})
                content = delta.get("content")
                if content:
                    content_parts.append(content)
            except json.JSONDecodeError:
                continue

        return "".join(content_parts)


openrouter = LLMClient(
    url="https://openrouter.ai/api/v1/chat/completions",
    env_var="OPEN_ROUTER_KEY",
    default_model="minimax/minimax-m2.5",
)

nim = LLMClient(
    url="https://integrate.api.nvidia.com/v1/chat/completions",
    env_var="NVIDIA_API_KEY",
    default_model="moonshotai/kimi-k2.5",
)


def chat(messages: list[dict], **kwargs) -> str:
    """Convenience function that delegates to the OpenRouter client."""
    return openrouter.chat(messages, **kwargs)
