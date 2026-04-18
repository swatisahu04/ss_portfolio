"""
Provider-agnostic LLM wrapper.

Supports Anthropic Claude and OpenAI GPT via env var `LLM_PROVIDER`.
All costs are tracked in AgentState for observability.

Also supports a `mock` provider for deterministic tests / dry-run CI eval.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass

from agent.config import get_agent_settings

# In-memory cache (per-process) for eval runs. Keyed on SHA256(provider|model|prompt).
_CACHE: dict[str, "LLMResponse"] = {}


@dataclass
class LLMResponse:
    text: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    cached: bool = False


# Rough public pricing per 1M tokens (USD) — update as prices change
PRICING = {
    "claude-sonnet-4-6": {"in": 3.00, "out": 15.00},
    "claude-opus-4-6":   {"in": 15.00, "out": 75.00},
    "claude-haiku-4-5":  {"in": 1.00, "out": 5.00},
    "gpt-4o-mini":       {"in": 0.15, "out": 0.60},
    "gpt-4o":            {"in": 2.50, "out": 10.00},
}


def _cost(model: str, in_tok: int, out_tok: int) -> float:
    p = PRICING.get(model, {"in": 0, "out": 0})
    return (in_tok * p["in"] + out_tok * p["out"]) / 1_000_000


def _cache_key(provider: str, model: str, prompt: str, system: str) -> str:
    h = hashlib.sha256()
    h.update(f"{provider}|{model}".encode())
    h.update(prompt.encode())
    h.update(system.encode())
    return h.hexdigest()


def complete(prompt: str, system: str = "", max_tokens: int = 1024) -> LLMResponse:
    s = get_agent_settings()
    provider = s.llm_provider
    model = s.llm_model

    if os.getenv("AGENT_LLM_MOCK") == "1":
        return _mock_complete(prompt)

    key = _cache_key(provider, model, prompt, system)
    if s.agent_cache_enabled and key in _CACHE:
        r = _CACHE[key]
        return LLMResponse(r.text, r.input_tokens, r.output_tokens, r.cost_usd, cached=True)

    if provider == "anthropic":
        resp = _call_anthropic(prompt, system, model, max_tokens)
    elif provider == "openai":
        resp = _call_openai(prompt, system, model, max_tokens)
    else:
        raise ValueError(f"unknown LLM_PROVIDER: {provider}")

    if s.agent_cache_enabled:
        _CACHE[key] = resp
    return resp


def _call_anthropic(prompt: str, system: str, model: str, max_tokens: int) -> LLMResponse:
    from anthropic import Anthropic

    client = Anthropic()
    resp = client.messages.create(
        model=model,
        system=system or "You are a helpful, concise assistant.",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(b.text for b in resp.content if hasattr(b, "text"))
    in_tok = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens
    return LLMResponse(
        text=text, input_tokens=in_tok, output_tokens=out_tok,
        cost_usd=_cost(model, in_tok, out_tok),
    )


def _call_openai(prompt: str, system: str, model: str, max_tokens: int) -> LLMResponse:
    from openai import OpenAI

    client = OpenAI()
    resp = client.chat.completions.create(
        model=model,
        max_tokens=max_tokens,
        messages=[
            {"role": "system", "content": system or "You are a helpful, concise assistant."},
            {"role": "user", "content": prompt},
        ],
    )
    text = resp.choices[0].message.content or ""
    in_tok = resp.usage.prompt_tokens
    out_tok = resp.usage.completion_tokens
    return LLMResponse(
        text=text, input_tokens=in_tok, output_tokens=out_tok,
        cost_usd=_cost(model, in_tok, out_tok),
    )


def _mock_complete(prompt: str) -> LLMResponse:
    """Deterministic stub — used in CI / unit tests with AGENT_LLM_MOCK=1."""
    if "Return a JSON object" in prompt and "intent" in prompt:
        return LLMResponse(json.dumps({
            "intent": "analytical",
            "marts_required": ["fct_portfolio_performance_daily"],
            "time_context": "ytd",
        }))
    if "produce a single PostgreSQL SELECT" in prompt.lower():
        return LLMResponse(
            "SELECT portfolio_id, ytd_return FROM marts.fct_portfolio_performance_daily "
            "WHERE portfolio_id = 'P-001' ORDER BY as_of_date DESC LIMIT 1;"
        )
    if "summarize the result" in prompt.lower():
        return LLMResponse("Portfolio P-001 YTD return: [cell:ytd_return].")
    return LLMResponse("(mock)")
