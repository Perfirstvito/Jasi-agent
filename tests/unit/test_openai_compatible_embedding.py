from __future__ import annotations

import json

import httpx
import pytest

from jasi.adapters.llm.openai_compatible_embedding import (
    EmbeddingFailure,
    OpenAICompatibleEmbedding,
)


@pytest.mark.asyncio
async def test_embedding_adapter_preserves_input_order_and_openai_contract() -> None:
    async def handle(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("https://embedding.example/v1/embeddings")
        assert request.headers["Authorization"] == "Bearer secret"
        assert json.loads(request.content) == {
            "model": "embedding-model",
            "input": ["first", "second"],
            "dimensions": 3,
        }
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": 1, "embedding": [0, 1, 0]},
                    {"index": 0, "embedding": [1, 0, 0]},
                ]
            },
        )

    adapter = OpenAICompatibleEmbedding(
        base_url="https://embedding.example/v1",
        api_key="secret",
        model="embedding-model",
        dimensions=3,
        timeout_seconds=5,
        transport=httpx.MockTransport(handle),
    )
    try:
        assert await adapter.embed(("first", "second")) == (
            (1.0, 0.0, 0.0),
            (0.0, 1.0, 0.0),
        )
    finally:
        await adapter.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data,error",
    [
        ([{"index": 0, "embedding": [1, 0, 0]}], "wrong result count"),
        (
            [
                {"index": 0, "embedding": [1, 0, 0]},
                {"index": 0, "embedding": [0, 1, 0]},
            ],
            "invalid result indexes",
        ),
        (
            [
                {"index": 0, "embedding": [1, 0]},
                {"index": 1, "embedding": [0, 1, 0]},
            ],
            "wrong vector dimensions",
        ),
        (
            [
                {"index": 0, "embedding": [1, 0, "NaN"]},
                {"index": 1, "embedding": [0, 1, 0]},
            ],
            "non-finite vector values",
        ),
    ],
)
async def test_embedding_adapter_rejects_invalid_results(data: list[dict], error: str) -> None:
    transport = httpx.MockTransport(lambda _request: httpx.Response(200, json={"data": data}))
    adapter = OpenAICompatibleEmbedding(
        base_url="https://embedding.example/v1",
        api_key="secret",
        model="embedding-model",
        dimensions=3,
        timeout_seconds=5,
        transport=transport,
    )
    try:
        with pytest.raises(EmbeddingFailure, match=error):
            await adapter.embed(("first", "second"))
    finally:
        await adapter.aclose()
