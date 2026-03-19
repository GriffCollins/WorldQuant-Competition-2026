"""
Team dashboard — run anytime to see progress across all bots.
    python dashboard.py
"""

import os
from dotenv import load_dotenv
from db import SupabaseDB

load_dotenv()

db = SupabaseDB(
    url=os.environ["SUPABASE_URL"],
    anon_key=os.environ["SUPABASE_ANON_KEY"],
)

attempted = db.get_attempted_count()
passing   = db.get_passing_count()

rows_results = db._get("results", {"select": "sharpe,fitness,turnover,passed,created_at"})
rows_sub     = db._get("submitted", {"select": "hash,bot_id,created_at"})
rows_bots    = db._get("attempted", {"select": "bot_id"})

# Per-bot breakdown
bot_counts: dict[str, int] = {}
for r in rows_bots:
    b = r.get("bot_id", "unknown")
    bot_counts[b] = bot_counts.get(b, 0) + 1

sharpes = [r["sharpe"] for r in rows_results if r.get("sharpe") is not None]
pass_rate = (passing / attempted * 100) if attempted else 0

print("\n══════════════════════════════════════")
print("       exSIF WQ Bot — Team Dashboard  ")
print("══════════════════════════════════════")
print(f"  Total simulated : {attempted}")
print(f"  Passing alphas  : {passing}  ({pass_rate:.1f}%)")
print(f"  Submitted       : {len(rows_sub)}")
if sharpes:
    print(f"  Best Sharpe     : {max(sharpes):.3f}")
    print(f"  Mean Sharpe     : {sum(sharpes)/len(sharpes):.3f}")
print()
print("  Per-bot activity:")
for bot, count in sorted(bot_counts.items()):
    print(f"    {bot:<20} {count} attempts")
print("══════════════════════════════════════\n")

if rows_results:
    passing_rows = [r for r in rows_results if r.get("passed")]
    if passing_rows:
        print("  Top 10 passing alphas by Sharpe:")
        top = sorted(passing_rows, key=lambda r: r.get("sharpe") or 0, reverse=True)[:10]
        for i, r in enumerate(top, 1):
            print(f"  {i:>2}. sharpe={r['sharpe']:.3f}  fitness={r['fitness']:.3f}  turnover={r['turnover']:.3f}")
        print()
