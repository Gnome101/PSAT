"""NVIDIA NIM client using requests."""

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

NIM_URL = "https://integrate.api.nvidia.com/v1/chat/completions"
DEFAULT_MODEL = "moonshotai/kimi-k2.5"


def _get_api_key() -> str:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    key = os.getenv("NVIDIA_API_KEY")
    if not key:
        raise RuntimeError("NVIDIA_API_KEY not set in .env")
    return key


def chat(messages: list[dict], model: str = DEFAULT_MODEL, **kwargs) -> str:
    """Send a chat completion and return the full response text.

    Streams the response and collects the content chunks.
    """
    api_key = _get_api_key()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "text/event-stream",
    }

    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": kwargs.get("max_tokens", 16384),
        "temperature": kwargs.get("temperature", 0.2),
        "top_p": kwargs.get("top_p", 0.9),
        "stream": True,
    }

    response = requests.post(NIM_URL, headers=headers, json=payload, stream=True, timeout=120)
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
