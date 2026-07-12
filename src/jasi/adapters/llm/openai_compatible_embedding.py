from __future__ import annotations

import math
from typing import Any

import httpx


class EmbeddingFailure(RuntimeError):
    pass


class OpenAICompatibleEmbedding:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        dimensions: int,
        timeout_seconds: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not base_url.strip() or not api_key.strip() or not model.strip():
            raise ValueError("embedding endpoint, API key, and model are required")
        if dimensions <= 0:
            raise ValueError("embedding dimensions must be positive")
        if timeout_seconds <= 0:
            raise ValueError("embedding timeout must be positive")
        self._model = model
        self._dimensions = dimensions
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_seconds,
            transport=transport,
        )

    @property
    def model_name(self) -> str:
        return self._model

    async def embed(self, texts: tuple[str, ...]) -> tuple[tuple[float, ...], ...]:
        if not texts:
            return ()
        payload: dict[str, Any] = {
            "model": self._model,
            "input": list(texts),
            "dimensions": self._dimensions,
        }
        try:
            response = await self._client.post("/embeddings", json=payload)
        except httpx.HTTPError as exc:
            raise EmbeddingFailure("embedding endpoint unavailable") from exc
        if response.is_error:
            raise EmbeddingFailure(_safe_http_error(response))
        try:
            body = response.json()
            rows = sorted(body["data"], key=lambda item: int(item["index"]))
            indexes = tuple(int(row["index"]) for row in rows)
            vectors = tuple(tuple(float(value) for value in row["embedding"]) for row in rows)
        except (KeyError, TypeError, ValueError) as exc:
            raise EmbeddingFailure("embedding endpoint returned an invalid response") from exc
        if len(vectors) != len(texts):
            raise EmbeddingFailure("embedding endpoint returned the wrong result count")
        if indexes != tuple(range(len(texts))):
            raise EmbeddingFailure("embedding endpoint returned invalid result indexes")
        if any(len(vector) != self._dimensions for vector in vectors):
            raise EmbeddingFailure("embedding endpoint returned the wrong vector dimensions")
        if any(not math.isfinite(value) for vector in vectors for value in vector):
            raise EmbeddingFailure("embedding endpoint returned non-finite vector values")
        return vectors

    async def aclose(self) -> None:
        await self._client.aclose()


def _safe_http_error(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"embedding request failed with HTTP {response.status_code}"
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        return str(error.get("message") or "embedding request failed")[:500]
    return f"embedding request failed with HTTP {response.status_code}"
