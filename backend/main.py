import asyncio
import json
import random
import string
from datetime import datetime, timedelta

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

app = FastAPI(title="Market Data Stream API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
    expose_headers=["X-Total-Records"],
)


@app.middleware("http")
async def add_cross_origin_headers(request, call_next):
    response = await call_next(request)
    # Required so Angular app in cross-origin-isolated mode can fetch this API
    response.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
    return response


def _build_symbols(count: int = 100) -> list[str]:
    rng = random.Random(42)
    letters = string.ascii_uppercase
    symbols: set[str] = set()
    while len(symbols) < count:
        symbols.add("".join(rng.choices(letters, k=4)))
    return sorted(symbols)


SYMBOLS = _build_symbols()

_YEAR_START = datetime(2026, 1, 1)
_YEAR_SECONDS = int((datetime(2027, 1, 1) - _YEAR_START).total_seconds()) - 1


def _make_record(rng: random.Random) -> dict:
    ts = _YEAR_START + timedelta(seconds=rng.randint(0, _YEAR_SECONDS))
    return {
        "ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "symbol": rng.choice(SYMBOLS),
        "price": round(rng.uniform(1.0, 999.99), 2),
        "volume": rng.randint(100, 10_000_000),
    }


@app.get("/api/stream")
async def stream_records(n: int = 100_000):
    """Stream n market-data records as newline-delimited JSON."""

    async def generate():
        rng = random.Random()
        for i in range(n):
            yield json.dumps(_make_record(rng)) + "\n"
            # Yield to the event loop every 1 000 records so we don't block
            if i % 1_000 == 999:
                await asyncio.sleep(0)

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={"X-Total-Records": str(n)},
    )


@app.get("/api/symbols")
async def get_symbols() -> list[str]:
    return SYMBOLS


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}
