"""
Main bot loop.

Each team member runs this with their own WQ credentials.
All coordination (dedup, results) goes through shared Supabase DB.

Usage:
    python bot.py --bot-id griff

Or set env vars and run directly.
"""

import os
import time
import logging
import argparse
from dotenv import load_dotenv

from brain_client import BrainClient
from generator import generate
from db import SupabaseDB

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_bot(bot_id: str, batch_size: int = 5, max_iters: int = 0):
    """
    Main loop. Generates alphas, deduplicates against shared DB,
    simulates via WQ Brain, logs results, submits passing alphas.

    Args:
        bot_id:     Unique identifier for this bot instance (e.g. "griff")
        batch_size: How many candidates to generate per iteration
        max_iters:  Stop after N iterations (0 = run forever)
    """
    db = SupabaseDB(
        url=os.environ["SUPABASE_URL"],
        anon_key=os.environ["SUPABASE_ANON_KEY"],
    )
    wq = BrainClient(
        username=os.environ["WQ_USERNAME"],
        password=os.environ["WQ_PASSWORD"],
    )

    logger.info(f"Bot '{bot_id}' starting. Attempted so far: {db.get_attempted_count()}")

    iteration = 0
    passes = 0
    sims = 0

    while True:
        iteration += 1
        if max_iters and iteration > max_iters:
            break

        logger.info(f"── Iteration {iteration} | sims: {sims} | passes: {passes} ──")

        candidates = generate(n=batch_size, settings_variant=True)

        for c in candidates:
            expr = c["expression"]
            h = c["hash"]
            settings = c.get("settings", {})

            # ── Dedup check ──────────────────────────────────────────────────
            if db.already_attempted(h):
                logger.debug(f"Skip (seen): {h}")
                continue

            claimed = db.claim_attempt(h, expr, settings, bot_id)
            if not claimed:
                logger.debug(f"Skip (race): {h}")
                continue

            # ── Simulate ─────────────────────────────────────────────────────
            logger.info(f"Simulating: {expr[:80]}")
            result = wq.simulate(expr, settings)
            sims += 1

            if result.get("status") in ("ERROR", "SUBMIT_FAILED", "TIMEOUT"):
                logger.warning(f"Sim failed ({result.get('status')}): {expr[:60]}")
                db.log_result(h, {}, passed=False)
                continue

            stats = wq.extract_stats(result)
            passed = wq.passes_thresholds(result)

            logger.info(
                f"  sharpe={stats.get('sharpe') or 0:.3f}  "
                f"fitness={stats.get('fitness') or 0:.3f}  "
                f"turnover={stats.get('turnover') or 0:.3f}  "
                f"{'PASS ✓' if passed else 'fail'}"
            )

            db.log_result(h, stats, passed=passed)

            # ── Submit if passing ─────────────────────────────────────────────
            if passed:
                passes += 1
                logger.info(f"  → Submitting alpha")
                submit_result = wq.submit_alpha(result)
                wq_alpha_id = submit_result.get("id", "unknown")
                db.log_submission(h, wq_alpha_id, bot_id)
                logger.info(f"  → Submitted: {wq_alpha_id}")

            # Small delay between sims to be polite to the API
            time.sleep(2)

        # Brief pause between batches
        time.sleep(5)

    logger.info(f"Bot '{bot_id}' finished. Total sims: {sims}, passes: {passes}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bot-id", default="bot1", help="Unique name for this bot")
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--max-iters", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    run_bot(
        bot_id=args.bot_id,
        batch_size=args.batch_size,
        max_iters=args.max_iters,
    )
