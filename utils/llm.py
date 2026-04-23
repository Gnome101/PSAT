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

        response = requests.post(self.url, headers=headers, json=payload, stream=True, timeout=120)
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
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                delta = choices[0].get("delta", {})
                content = delta.get("content")
                if content:
                    content_parts.append(content)
            except json.JSONDecodeError:
                continue

        return "".join(content_parts)


openrouter = LLMClient(
    url="https://openrouter.ai/api/v1/chat/completions",
    env_var="OPEN_ROUTER_KEY",
    default_model="google/gemini-2.0-flash-001",
)


def chat(messages: list[dict], **kwargs) -> str:
    """Convenience function that delegates to the OpenRouter client."""
    return openrouter.chat(messages, **kwargs)


def chat_with_tools_stream(
    messages: list[dict],
    tools: list[dict],
    tool_impls: dict,
    *,
    model: str | None = None,
    max_iters: int = 6,
    max_tokens: int = 1200,
    temperature: float = 0.3,
):
    """Generator version of chat_with_tools.

    Yields event dicts so callers can surface tool-call progress to a
    streaming frontend. Event schema (one dict per yield):

        {"event": "iteration", "n": 1}
        {"event": "tool_call",     "name": "list_contracts", "args": {...}}
        {"event": "tool_result",   "name": "list_contracts", "summary": "27 contracts"}
        {"event": "answer",        "text": "<final assistant prose>"}
        {"event": "error",         "message": "<reason>"}

    The final event is always either ``answer`` or ``error``. Consumers
    can stop iterating after those.
    """
    import os
    from pathlib import Path

    import requests
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    api_key = os.getenv("OPEN_ROUTER_KEY")
    if not api_key:
        yield {"event": "error", "message": "OPEN_ROUTER_KEY not set"}
        return

    transcript = list(messages)
    for iter_n in range(1, max_iters + 1):
        yield {"event": "iteration", "n": iter_n}
        payload = {
            "model": model or openrouter.default_model,
            "messages": transcript,
            "tools": tools,
            "tool_choice": "auto",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,
        }
        try:
            resp = requests.post(
                openrouter.url,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            yield {"event": "error", "message": f"LLM request failed: {exc}"}
            return

        choice = data["choices"][0]
        message = choice["message"]
        transcript.append(message)
        tool_calls = message.get("tool_calls") or []
        if not tool_calls:
            yield {"event": "answer", "text": message.get("content") or ""}
            return
        for call in tool_calls:
            name = call["function"]["name"]
            args_raw = call["function"].get("arguments") or "{}"
            try:
                args = json.loads(args_raw) if isinstance(args_raw, str) else (args_raw or {})
            except json.JSONDecodeError:
                args = {}
            yield {"event": "tool_call", "name": name, "args": args}
            impl = tool_impls.get(name)
            if impl is None:
                result = {"error": f"unknown tool {name!r}"}
            else:
                try:
                    result = impl(**args)
                except Exception as exc:  # noqa: BLE001 — tools run user-shaped args
                    result = {"error": f"{type(exc).__name__}: {exc}"}
            # Build a short human-readable summary for the progress UI.
            # The frontend only uses ``summary``; the LLM still sees the
            # full result payload via the tool message below.
            summary = _summarize_tool_result(name, result)
            yield {"event": "tool_result", "name": name, "summary": summary}
            transcript.append(
                {
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "name": name,
                    "content": json.dumps(result),
                }
            )
    yield {"event": "error", "message": f"reached tool-call iteration cap ({max_iters})"}


def _summarize_tool_result(name: str, result: dict | list) -> str:
    """One-line human-readable summary for the progress UI."""
    if isinstance(result, dict) and result.get("error"):
        return f"error: {result['error']}"[:120]
    if not isinstance(result, dict):
        return f"{type(result).__name__} response"
    # Pick the first list-valued key as a count hint.
    for k, v in result.items():
        if isinstance(v, list) and k not in ("error",):
            return f"{len(v)} {k}"
    # Fallback: name of the first non-empty field.
    for k, v in result.items():
        if v and k != "error":
            return f"{k}={str(v)[:60]}"
    return "ok"


def chat_with_tools(
    messages: list[dict],
    tools: list[dict],
    tool_impls: dict,
    *,
    model: str | None = None,
    max_iters: int = 6,
    max_tokens: int = 1200,
    temperature: float = 0.3,
) -> tuple[str, list[dict]]:
    """Run an OpenAI-compatible tool-calling loop against OpenRouter.

    The loop:
      1. Send messages + tools to the LLM (non-streaming).
      2. If the response has no ``tool_calls``, return its content.
      3. Otherwise, execute each tool_call against ``tool_impls`` and
         append the result as a ``tool`` message, then loop.
      4. Cap at ``max_iters`` iterations to prevent runaway chains.

    Returns ``(answer_text, transcript)`` — the transcript is every
    message the conversation produced (useful for logging / audit).

    Non-streaming because tool-call chunks fragment inconveniently in
    SSE; one round-trip per turn is fine for a chat that fires once
    per user question.
    """
    import os
    from pathlib import Path

    import requests
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    api_key = os.getenv("OPEN_ROUTER_KEY")
    if not api_key:
        raise RuntimeError("OPEN_ROUTER_KEY not set")

    transcript = list(messages)
    for _ in range(max_iters):
        payload = {
            "model": model or openrouter.default_model,
            "messages": transcript,
            "tools": tools,
            "tool_choice": "auto",
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,
        }
        resp = requests.post(
            openrouter.url,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        choice = data["choices"][0]
        message = choice["message"]
        transcript.append(message)
        tool_calls = message.get("tool_calls") or []
        if not tool_calls:
            return (message.get("content") or ""), transcript
        # Execute each tool call and attach the result. Any tool that
        # raises turns into an error-shaped dict so the LLM can
        # recover on the next iteration instead of the loop crashing.
        for call in tool_calls:
            name = call["function"]["name"]
            args_raw = call["function"].get("arguments") or "{}"
            try:
                args = json.loads(args_raw) if isinstance(args_raw, str) else (args_raw or {})
            except json.JSONDecodeError:
                args = {}
            impl = tool_impls.get(name)
            if impl is None:
                result = {"error": f"unknown tool {name!r}"}
            else:
                try:
                    result = impl(**args)
                except Exception as exc:  # noqa: BLE001 — tools run user-shaped args
                    result = {"error": f"{type(exc).__name__}: {exc}"}
            transcript.append(
                {
                    "role": "tool",
                    "tool_call_id": call["id"],
                    "name": name,
                    "content": json.dumps(result),
                }
            )
    # Hit the iteration cap — return whatever the model last said.
    return (message.get("content") or "(assistant reached tool-call iteration cap)"), transcript
