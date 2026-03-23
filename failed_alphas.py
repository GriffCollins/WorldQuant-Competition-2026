"""
Failed Alpha Dashboard
Retrieves best-performing failed alphas from Supabase and displays them.

Usage:
    python failed_alphas.py
    python failed_alphas.py --min-sharpe 0.8 --sort fitness --limit 30
    python failed_alphas.py --fail-reason HIGH_TURNOVER
"""

import os
import json
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()

THRESHOLDS = {
    "sharpe":       1.25,
    "fitness":      1.0,
    "turnover_max": 0.70,
    "turnover_min": 0.01,
}

# ── Terminal colours ───────────────────────────────────────────────────────────
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    RED    = "\033[91m"
    YELLOW = "\033[93m"
    GREEN  = "\033[92m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"
    BG_DARK = "\033[100m"

def green(s):  return f"{C.GREEN}{s}{C.RESET}"
def yellow(s): return f"{C.YELLOW}{s}{C.RESET}"
def red(s):    return f"{C.RED}{s}{C.RESET}"
def bold(s):   return f"{C.BOLD}{s}{C.RESET}"
def dim(s):    return f"{C.DIM}{s}{C.RESET}"
def cyan(s):   return f"{C.CYAN}{s}{C.RESET}"

def colour_metric(key, val):
    if val is None:
        return dim("n/a")
    if key == "sharpe":
        fmt = f"{val:.3f}"
        return green(fmt) if val >= 1.25 else yellow(fmt) if val >= 0.8 else red(fmt)
    if key == "fitness":
        fmt = f"{val:.3f}"
        return green(fmt) if val >= 1.0 else yellow(fmt) if val >= 0.5 else red(fmt)
    if key == "turnover":
        fmt = f"{val*100:.1f}%"
        return green(fmt) if val <= 0.70 else yellow(fmt) if val <= 0.90 else red(fmt)
    if key == "returns":
        fmt = f"{val*100:.2f}%"
        return green(fmt) if val > 0 else red(fmt)
    if key == "drawdown":
        fmt = f"{val*100:.2f}%"
        return green(fmt) if val < 0.15 else yellow(fmt) if val < 0.30 else red(fmt)
    return str(val)

def bar(val, max_val, width=20, colour_fn=None):
    filled = int(min(val / max_val, 1.0) * width)
    bar_str = "█" * filled + "░" * (width - filled)
    return colour_fn(bar_str) if colour_fn else bar_str

def sharpe_colour_fn(s):
    v = float(s.replace("█", "1").replace("░", "0") or 0)
    return green

# ── Supabase fetch ─────────────────────────────────────────────────────────────

def fetch_table(base_url, key, table, select):
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    r = requests.get(
        f"{base_url}/rest/v1/{table}",
        headers=headers,
        params={"select": select},
    )
    r.raise_for_status()
    return r.json()


def load_failed_alphas(url, key):
    print(dim("Fetching attempted..."))
    attempted = fetch_table(url, key, "attempted", "hash,expression,settings")

    print(dim("Fetching results..."))
    results = fetch_table(url, key, "results", "hash,sharpe,fitness,turnover,returns,drawdown,passed,sim_id")

    result_map = {r["hash"]: r for r in results}

    merged = []
    for a in attempted:
        r = result_map.get(a["hash"], {})
        if r.get("sharpe") is None:
            continue

        settings = a.get("settings", {})
        if isinstance(settings, str):
            try:
                settings = json.loads(settings)
            except Exception:
                settings = {}

        merged.append({
            "hash":       a["hash"],
            "expression": a.get("expression", a["hash"]),
            "settings":   settings,
            "sharpe":     r.get("sharpe"),
            "fitness":    r.get("fitness"),
            "turnover":   r.get("turnover"),
            "returns":    r.get("returns"),
            "drawdown":   r.get("drawdown"),
            "passed":     r.get("passed", False),
            "sim_id":     r.get("sim_id"),
        })

    return merged


def fail_reasons(row):
    reasons = []
    if (row["sharpe"] or 0) < THRESHOLDS["sharpe"]:
        reasons.append("LOW_SHARPE")
    if (row["fitness"] or 0) < THRESHOLDS["fitness"]:
        reasons.append("LOW_FITNESS")
    if (row["turnover"] or 0) > THRESHOLDS["turnover_max"]:
        reasons.append("HIGH_TURNOVER")
    if (row["turnover"] or 1) < THRESHOLDS["turnover_min"]:
        reasons.append("LOW_TURNOVER")
    return reasons


# ── Display ────────────────────────────────────────────────────────────────────

def print_card(row, rank):
    reasons = fail_reasons(row)
    s = row["settings"]

    reason_str = "  ".join(
        yellow(f"▲ {r}") if r == "HIGH_TURNOVER" else red(f"✗ {r}")
        for r in reasons
    )

    width = 80
    print("─" * width)
    print(f"{bold(cyan(f'#{rank}'))}  {bold(row['expression'])}")
    print(f"     {reason_str}")
    print()

    # Metrics
    sharpe   = row["sharpe"]   or 0
    fitness  = row["fitness"]  or 0
    turnover = row["turnover"] or 0
    returns  = row["returns"]  or 0
    drawdown = row["drawdown"] or 0

    sharpe_bar   = bar(sharpe,   2.5,  colour_fn=lambda x: green(x) if sharpe >= 1.25 else yellow(x) if sharpe >= 0.8 else red(x))
    fitness_bar  = bar(fitness,  2.0,  colour_fn=lambda x: green(x) if fitness >= 1.0 else yellow(x) if fitness >= 0.5 else red(x))
    turnover_bar = bar(turnover, 1.0,  colour_fn=lambda x: green(x) if turnover <= 0.7 else yellow(x) if turnover <= 0.9 else red(x))

    print(f"  {'Sharpe':<12} {colour_metric('sharpe', sharpe):<20}  {sharpe_bar}  (need ≥ 1.25)")
    print(f"  {'Fitness':<12} {colour_metric('fitness', fitness):<20}  {fitness_bar}  (need ≥ 1.00)")
    print(f"  {'Turnover':<12} {colour_metric('turnover', turnover):<20}  {turnover_bar}  (need ≤ 0.70)")
    print(f"  {'Returns':<12} {colour_metric('returns', returns)}")
    print(f"  {'Drawdown':<12} {colour_metric('drawdown', drawdown)}")
    print()

    # Settings
    region  = s.get("region", "–")
    univ    = s.get("universe", "–")
    neutral = s.get("neutralization", "–")
    decay   = s.get("decay", "–")
    trunc   = f"{s.get('truncation', 0)*100:.0f}%" if s.get("truncation") else "–"
    delay   = s.get("delay", "–")

    print(f"  {dim('Region')} {region}   "
          f"{dim('Universe')} {univ}   "
          f"{dim('Neutral')} {neutral}   "
          f"{dim('Decay')} {decay}   "
          f"{dim('Trunc')} {trunc}   "
          f"{dim('Delay')} {delay}")
    print(f"  {dim('sim_id')} {dim(row.get('sim_id') or '–')}")


def print_summary(data, total_attempted, total_passed):
    width = 80
    print("═" * width)
    print(bold("  Failed Alpha Dashboard — exSIF / IQC 2026"))
    print("═" * width)
    print(f"  Total simulated : {bold(str(total_attempted))}")
    print(f"  Passed          : {bold(green(str(total_passed)))}")
    print(f"  Failed (shown)  : {bold(str(len(data)))}")
    hit_rate = (total_passed / total_attempted * 100) if total_attempted else 0
    print(f"  Hit rate        : {bold(f'{hit_rate:.1f}%')}")

    if data:
        sharpes = [r["sharpe"] for r in data if r["sharpe"]]
        best    = max(sharpes)
        mean    = sum(sharpes) / len(sharpes)
        print(f"  Best Sharpe     : {colour_metric('sharpe', best)}")
        print(f"  Mean Sharpe     : {colour_metric('sharpe', mean)}")

        high_turnover = sum(1 for r in data if "HIGH_TURNOVER" in fail_reasons(r))
        low_sharpe    = sum(1 for r in data if "LOW_SHARPE"    in fail_reasons(r))
        low_fitness   = sum(1 for r in data if "LOW_FITNESS"   in fail_reasons(r))
        print(f"\n  Failure breakdown:")
        print(f"    {yellow('HIGH_TURNOVER')}  {high_turnover}")
        print(f"    {red('LOW_SHARPE')}      {low_sharpe}")
        print(f"    {red('LOW_FITNESS')}     {low_fitness}")
    print("═" * width)
    print()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Display best failed alphas from Supabase")
    parser.add_argument("--min-sharpe",    type=float, default=0.5,         help="Minimum Sharpe to show (default 0.5)")
    parser.add_argument("--sort",          default="sharpe",                 help="Sort by: sharpe, fitness, returns (default sharpe)")
    parser.add_argument("--limit",         type=int,   default=20,           help="Max alphas to display (default 20)")
    parser.add_argument("--fail-reason",   default="all",                    help="Filter by: HIGH_TURNOVER, LOW_SHARPE, LOW_FITNESS, all")
    parser.add_argument("--universe",      default="all",                    help="Filter by universe e.g. TOP1000")
    parser.add_argument("--region",        default="all",                    help="Filter by region e.g. USA")
    args = parser.parse_args()

    url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_ANON_KEY", "")
    if not url or not key:
        print(red("Error: SUPABASE_URL and SUPABASE_ANON_KEY must be set in .env"))
        return

    try:
        all_alphas = load_failed_alphas(url, key)
    except requests.HTTPError as e:
        print(red(f"Supabase error: {e}"))
        return

    total_attempted = len(all_alphas)
    total_passed    = sum(1 for r in all_alphas if r["passed"])

    # Filter to failed only
    data = [r for r in all_alphas if not r["passed"]]

    # Apply filters
    data = [r for r in data if (r["sharpe"] or 0) >= args.min_sharpe]

    if args.fail_reason != "all":
        data = [r for r in data if args.fail_reason in fail_reasons(r)]

    if args.universe != "all":
        data = [r for r in data if r["settings"].get("universe") == args.universe]

    if args.region != "all":
        data = [r for r in data if r["settings"].get("region") == args.region]

    # Sort
    sort_key = args.sort if args.sort in ("sharpe", "fitness", "returns") else "sharpe"
    data.sort(key=lambda r: r.get(sort_key) or 0, reverse=True)
    data = data[:args.limit]

    print_summary(data, total_attempted, total_passed)

    if not data:
        print(yellow("No results match the current filters."))
        return

    for i, row in enumerate(data, 1):
        print_card(row, i)

    print("─" * 80)
    print(dim(f"  Showing {len(data)} results. Use --limit, --min-sharpe, --fail-reason to filter."))
    print()


if __name__ == "__main__":
    main()
