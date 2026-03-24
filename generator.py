"""
Family-aware alpha generator for WorldQuant BRAIN research.

Design:
- Mutates only inside three validated economic families:
    A = intraday / microstructure reversion
    B = short-horizon return reversal
    C = slow fundamental + hybrid value overlays
"""

import random
import hashlib
import re

FAMILY_A = [
    "-rank(ts_mean((close-vwap)/vwap,3) * log(ts_mean(volume,5)))",
    "ts_decay_linear(-rank(ts_mean((close-vwap)/vwap,3) * log(ts_mean(volume,5))),6)",
    "-rank(ts_mean((close-vwap)/vwap,3) * rank(volume/ts_mean(volume,20)))",
    "-rank((ts_mean(close,3)-vwap)/vwap * rank(volume/ts_mean(volume,20)))",
    "-rank(ts_delay((close-vwap)/vwap,1) * log(ts_mean(volume,5)))",
    "rank(ts_mean((open-close)/open,3))",
    "ts_decay_linear(rank(open/close - 1),5)",
    "-rank(ts_mean(open/close - 1,5) * rank(volume/adv20))",
    "-rank(ts_mean(close/open - 1,3))",
    "-rank((close-vwap)/ts_stddev(returns,20))",
]

FAMILY_B = [
    "-rank(ts_mean(returns,3))",
    "-rank(ts_mean(returns,5))",
    "ts_decay_linear(-rank(ts_mean(returns,3)),5)",
    "-rank(ts_zscore(returns,20))",
    "-rank(ts_zscore(returns,40))",
    "-rank(ts_rank(returns,10))",
    "-rank(ts_rank(returns,15))",
    "-rank(ts_delta(close,3))",
    "ts_decay_linear(-rank(ts_delta(close,3)),5)",
    "-rank(ts_delta(close,5))",
]

FAMILY_C = [
    "-rank(ts_zscore(debt,40)) + -rank(ts_mean((close-vwap)/vwap,3))",
    "-rank(close/debt) + -rank(ts_mean(returns,3))",
    "rank(sales/assets) + -rank(ts_mean((close-vwap)/vwap,3))",
    "-rank(ts_zscore(debt,40) * ts_mean(returns,3))",
    "-rank(close/bookvalue) + -rank(ts_mean(returns,3))",
    "rank(cashflow/assets) + -rank(ts_mean((close-vwap)/vwap,3))",
]

FAMILIES = {"A": FAMILY_A, "B": FAMILY_B, "C": FAMILY_C}

WINDOW_MAP = {
    "3": ["3", "5"],
    "5": ["3", "5", "10"],
    "20": ["15", "20", "30"],
    "40": ["30", "40", "60"],
}

def mutate_windows(expr):
    for k, vals in WINDOW_MAP.items():
        expr = re.sub(rf'(?<!\d){k}(?!\d)', random.choice(vals), expr, count=1)
    return expr

def mutate_volume(expr):
    return expr.replace("volume", random.choice(["volume", "adv20", "adv60"]), 1)

def mutate(expr):
    expr = mutate_windows(expr)
    if "volume" in expr:
        expr = mutate_volume(expr)
    return expr

def expr_hash(expr):
    norm = re.sub(r"\s+", "", expr.lower())
    return hashlib.sha256(norm.encode()).hexdigest()[:16]

def random_settings():
    return {
        "region": "USA",
        "universe": random.choice(["TOP3000", "TOP1000"]),
        "neutralization": random.choice(["MARKET", "SECTOR", "SUBINDUSTRY"]),
        "decay": random.choice([4, 6, 8, 10]),
        "truncation": random.choice([0.05, 0.08, 0.10]),
        "delay": 1,
        "pasteurization": "ON",
        "unitHandling": "VERIFY",
        "nanHandling": "OFF",
        "language": "FASTEXPR",
    }

def generate(n=20):
    out = []
    seen = set()
    while len(out) < n:
        fam = random.choice(list(FAMILIES.keys()))
        expr = mutate(random.choice(FAMILIES[fam]))
        h = expr_hash(expr)
        if h in seen:
            continue
        seen.add(h)
        out.append({
            "family": fam,
            "expression": expr,
            "hash": h,
            "settings": random_settings()
        })
    return out

if __name__ == "__main__":
    for x in generate(25):
        print(x)
