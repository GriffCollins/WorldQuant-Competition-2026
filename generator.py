"""
Alpha generator — Level 1 template mutation.

Templates are drawn from WQ101 and common community patterns.
Each template is parameterised over operators, fields, and windows.
The generate() function produces syntactically valid expressions
with dramatically better hit rates than pure random.
"""

import random
import hashlib
import re
from itertools import product as iproduct

# ── Data fields ────────────────────────────────────────────────────────────────

PRICE_FIELDS = ["close", "open", "high", "low", "vwap", "returns"]
VOLUME_FIELDS = ["volume", "adv20", "adv60", "adv120"]
FUNDAMENTAL_FIELDS = [
    "sales", "assets", "ebitda", "debt", "equity", "cashflow",
    "netincome", "eps", "bookvalue", "dividends",
]
ALL_FIELDS = PRICE_FIELDS + VOLUME_FIELDS + FUNDAMENTAL_FIELDS

SHORT_WINDOWS  = [3, 5, 10]
MEDIUM_WINDOWS = [15, 20, 30]
LONG_WINDOWS   = [40, 60, 120]
ALL_WINDOWS    = SHORT_WINDOWS + MEDIUM_WINDOWS + LONG_WINDOWS

# ── Alpha templates ────────────────────────────────────────────────────────────
# Use {price}, {price2}, {volume}, {fundamental}, {ws}, {wm}, {wl}
# ws = short window, wm = medium window, wl = long window

TEMPLATES = [
    # Momentum / trend
    "-rank(ts_delta({price}, {wm}))",
    "rank(ts_delta({price}, {wm})) * -1",
    "-rank(ts_momentum({price}, {ws}, {wm}))",
    "rank(ts_mean({price}, {ws}) / ts_mean({price}, {wl}) - 1)",
    "-rank(ts_rank({price}, {wm}))",
    "rank(ts_zscore({price}, {wl})) * -1",

    # Reversal
    "rank(ts_delta({price}, {ws})) * -1",
    "-rank(ts_decay_linear(ts_delta({price}, {ws}), {ws}))",
    "rank({price} / ts_mean({price}, {wm}) - 1) * -1",
    "-rank(ts_delta(log({price}), {ws}))",

    # Volume / price interaction
    "-rank(ts_corr(rank({price}), rank({volume}), {wm}))",
    "rank(-ts_corr({price}, {volume}, {ws}))",
    "-rank(ts_corr(vwap, volume, {wm}))",
    "rank(ts_corr(ts_delta({price}, {ws}), {volume}, {wm}))",
    "-rank(({price} - vwap) / vwap * log({volume}))",

    # Volatility
    "-rank(ts_std({price}, {wm}))",
    "rank(1 / ts_std(returns, {wm}))",
    "-rank(ts_std(returns, {ws}) / ts_std(returns, {wl}))",
    "rank(ts_skewness(returns, {wm})) * -1",
    "-rank(ts_kurtosis(returns, {wm}))",

    # Fundamental value
    "rank({fundamental} / close)",
    "-rank(close / {fundamental})",
    "rank(ts_delta({fundamental}, {wl}) / {fundamental})",
    "-rank(ts_zscore({fundamental}, {wl}))",
    "rank({fundamental} / assets)",

    # Cross-sectional
    "-rank(close / ts_mean(close, {wm}))",
    "rank(zscore(returns))",
    "-rank(ts_rank(returns, {wm}))",
    "rank(ts_mean(returns, {ws}) - ts_mean(returns, {wl}))",

    # Composite momentum + volume
    "-rank(ts_corr(ts_rank({price}, {wm}), ts_rank({volume}, {wm}), {ws}))",
    "rank(ts_mean(returns, {ws}) / ts_std(returns, {wm}))",
    "-rank(ts_decay_linear(rank(returns), {ws}))",

    # High-low range
    "rank(-(high - low) / close)",
    "-rank(ts_mean(high - low, {wm}) / close)",
    "rank((close - low) / (high - low + 0.001))",
    "-rank(ts_delta(high - low, {ws}))",

    # Open-close overnight / intraday
    "rank(open / close - 1)",
    "-rank(ts_mean(open / close - 1, {wm}))",
    "rank(close / open - 1) * -1",
]


# ── Normalisation ──────────────────────────────────────────────────────────────

def _normalise(expr: str) -> str:
    """Canonicalise an expression for deduplication purposes."""
    expr = re.sub(r'\s+', '', expr)
    expr = expr.lower()
    # Bucket windows to reduce near-duplicate hits
    for w in sorted(ALL_WINDOWS, reverse=True):
        expr = re.sub(rf'\b{w}\b', _bucket_window(w), expr)
    return expr


def _bucket_window(w: int) -> str:
    if w <= 10:
        return "WS"
    if w <= 40:
        return "WM"
    return "WL"


def expr_hash(expr: str) -> str:
    return hashlib.sha256(_normalise(expr).encode()).hexdigest()[:16]


# ── Generator ──────────────────────────────────────────────────────────────────

def generate(n: int = 1, settings_variant: bool = True) -> list[dict]:
    """
    Generate n alpha candidates.
    Each candidate is a dict with 'expression', 'hash', and optional 'settings'.
    """
    candidates = []
    seen_hashes = set()
    attempts = 0

    while len(candidates) < n and attempts < n * 20:
        attempts += 1
        template = random.choice(TEMPLATES)
        expr = _fill_template(template)
        h = expr_hash(expr)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        candidate = {"expression": expr, "hash": h}
        if settings_variant:
            candidate["settings"] = _random_settings()
        candidates.append(candidate)

    return candidates


def _fill_template(template: str) -> str:
    ws = random.choice(SHORT_WINDOWS)
    wm = random.choice(MEDIUM_WINDOWS)
    wl = random.choice(LONG_WINDOWS)

    # Pick two distinct prices for templates using {price2}
    prices = random.sample(PRICE_FIELDS, 2)

    return template.format(
        price=prices[0],
        price2=prices[1],
        volume=random.choice(VOLUME_FIELDS),
        fundamental=random.choice(FUNDAMENTAL_FIELDS),
        ws=ws,
        wm=wm,
        wl=wl,
    )


def _random_settings() -> dict:
    """Vary simulation settings to explore more of the space."""
    return {
        "region": random.choice(["USA", "USA"]),  # extend later
        "universe": random.choice(["TOP3000", "TOP2000", "TOP1000"]),
        "neutralization": random.choice(["MARKET", "SECTOR", "SUBINDUSTRY"]),
        "decay": random.choice([0, 2, 4, 6]),
        "truncation": random.choice([0.05, 0.08, 0.10]),
        "delay": 1,
        "pasteurization": "ON",
        "unitHandling": "VERIFY",
        "nanHandling": "OFF",
        "language": "FASTEXPR",
        "visualization": False,
        "instrumentType": "EQUITY",
    }


# ── Quick test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    for c in generate(10):
        print(c["hash"], c["expression"])
