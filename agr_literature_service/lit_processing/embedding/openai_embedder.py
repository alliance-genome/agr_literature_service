"""OpenAI implementation of the shared ``Embedder`` contract (SCRUM-6142).

Wraps the OpenAI embeddings API behind
:class:`agr_abc_document_parsers.embeddings.Embedder`, so the ABC producer can
plug it into any chunker polymorphically. Batched with retry/backoff; exposes
``model_name`` / ``dimension`` and a tiktoken-based ``count_tokens`` for the
model's token preflight.

``openai`` / ``tiktoken`` are optional runtime deps: this module is imported
lazily by :mod:`.embedding_generation`, which degrades gracefully when the
embedding stack is unavailable.
"""

import time
from typing import List, Optional, Sequence

from agr_abc_document_parsers.embeddings import Embedder

DEFAULT_MODEL = "text-embedding-3-small"
DEFAULT_DIMENSION = 1536
# OpenAI's context limit for text-embedding-3-small, minus a safety margin, per
# the curation-assistant spec (§5). A chunk over this signals a chunking bug.
MODEL_TOKEN_LIMIT = 8191
TOKEN_SAFETY_MARGIN = 500


class OpenAIEmbedder(Embedder):
    """Embed chunk ``content`` with an OpenAI embedding model."""

    def __init__(self, api_key: str, *, model: str = DEFAULT_MODEL,
                 dimension: int = DEFAULT_DIMENSION, batch_size: int = 100,
                 max_retries: int = 5) -> None:
        self._model = model
        self._dimension = dimension
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._api_key = api_key
        self._client = None
        self._encoder = None

    @property
    def model_name(self) -> str:
        # Versioned, provider-qualified id recorded in the recipe descriptor.
        return f"openai:{self._model}"

    @property
    def model(self) -> str:
        return self._model

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def max_input_tokens(self) -> int:
        return MODEL_TOKEN_LIMIT - TOKEN_SAFETY_MARGIN

    def _get_client(self):  # pragma: no cover - thin OpenAI client factory
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(api_key=self._api_key)
        return self._client

    def _get_encoder(self):
        if self._encoder is None:
            import tiktoken
            self._encoder = tiktoken.encoding_for_model(self._model)
        return self._encoder

    def count_tokens(self, text: str) -> int:
        return len(self._get_encoder().encode(text or ""))

    def truncate_to_limit(self, text: str) -> str:
        """Truncate ``text`` to the model's safe token budget (for the optional
        whole-document vector, whose source text can exceed the limit)."""
        enc = self._get_encoder()
        tokens = enc.encode(text or "")
        if len(tokens) <= self.max_input_tokens:
            return text
        return enc.decode(tokens[:self.max_input_tokens])

    def embed(self, texts: Sequence[str]) -> List[List[float]]:
        """Return one 1536-d vector per input text, order preserved. Batched;
        each batch retried with exponential backoff on transient errors."""
        if not texts:
            return []
        client = self._get_client()
        out: List[List[float]] = []
        for start in range(0, len(texts), self._batch_size):
            batch = list(texts[start:start + self._batch_size])
            resp = self._create_with_retry(client, batch)
            for item in resp.data:
                vector = item.embedding
                if len(vector) != self._dimension:
                    raise ValueError(
                        f"unexpected embedding dim {len(vector)} (expected {self._dimension})"
                    )
                out.append([float(x) for x in vector])
        return out

    def _create_with_retry(self, client, batch: List[str]):  # pragma: no cover - network
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                return client.embeddings.create(model=self._model, input=batch)
            except Exception as exc:  # rate limit / transient
                last_exc = exc
                if attempt == self._max_retries - 1:
                    break
                time.sleep(2 ** attempt)
        raise RuntimeError(f"OpenAI embeddings failed after {self._max_retries} retries") from last_exc
