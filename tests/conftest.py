"""Shared fixtures. The `provider` fixture runs the fake provider on a real socket."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from fake_provider import FakeProvider, running


@pytest.fixture
def provider() -> Iterator[tuple[str, FakeProvider]]:
    p = FakeProvider()
    with running(p) as base_url:
        yield base_url, p


@pytest.fixture
def limited_provider() -> Iterator[tuple[str, FakeProvider]]:
    p = FakeProvider(rps_limit=5, tps_limit=10_000)
    with running(p) as base_url:
        yield base_url, p
