"""
Groq LLM helper for Medallion ETL MCP server.

Thin wrapper around Groq's chat.completions API that accepts LangChain
messages (SystemMessage, HumanMessage, etc.) and returns a single string.
"""

import os
from typing import List
from groq import AsyncGroq
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage


def get_groq_async_client() -> AsyncGroq:
    """Create an AsyncGroq client using GROQ_API_KEY from environment."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set in environment")
    return AsyncGroq(api_key=api_key)


async def groq_chat_complete(
    messages: List[BaseMessage],
    model_env_key: str,
    default_model: str,
    temperature_env_key: str = "GROQ_TEMPERATURE",
    max_tokens_env_key: str = "GROQ_MAX_TOKENS",
    top_p_env_key: str = "GROQ_TOP_P",
) -> str:
    """
    Call Groq chat completion with LangChain messages.

    Args:
        messages: List[BaseMessage] (SystemMessage, HumanMessage, etc.)
        model_env_key: env var name for model id (e.g. GROQ_MODEL_PLANNER)
        default_model: fallback model id if env var is missing
        temperature_env_key: env var for temperature (float)
        max_tokens_env_key: env var for max_tokens (int)
        top_p_env_key: env var for top_p (float)

    Returns:
        str: content of the first assistant message.
    """
    client = get_groq_async_client()

    model = os.getenv(model_env_key, default_model)
    temperature = float(os.getenv(temperature_env_key, "0.2"))
    max_tokens = int(os.getenv(max_tokens_env_key, "4096"))
    top_p = float(os.getenv(top_p_env_key, "0.95"))

    # Convert LangChain messages to OpenAI-compatible dicts
    payload_messages = []
    for m in messages:
        if isinstance(m, SystemMessage):
            role = "system"
        elif isinstance(m, HumanMessage):
            role = "user"
        else:
            role = m.type  # e.g. "assistant", etc.
        payload_messages.append({"role": role, "content": m.content})

    completion = await client.chat.completions.create(
        model=model,
        messages=payload_messages,
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
    )

    return completion.choices[0].message.content or ""
