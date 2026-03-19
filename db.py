"""
Supabase coordination layer.

Tables required (run setup_tables() once or paste SQL into Supabase editor):

  attempted (
    hash        TEXT PRIMARY KEY,
    expression  TEXT NOT NULL,
    settings    JSONB,
    bot_id      TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
  )

  results (
    hash        TEXT PRIMARY KEY REFERENCES attempted(hash),
    sharpe      REAL,
    fitness     REAL,
    turnover    REAL,
    margin      REAL,
    returns     REAL,
    drawdown    REAL,
    sim_id      TEXT,
    passed      BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
  )

  submitted (
    hash        TEXT PRIMARY KEY REFERENCES results(hash),
    wq_alpha_id TEXT,
    bot_id      TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
  )
"""

import os
import json
import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)


class SupabaseDB:
    def __init__(self, url: str, anon_key: str):
        self.base = url.rstrip("/") + "/rest/v1"
        self.headers = {
            "apikey": anon_key,
            "Authorization": f"Bearer {anon_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def _get(self, table: str, params: dict) -> list:
        r = requests.get(
            f"{self.base}/{table}",
            headers={**self.headers, "Prefer": ""},
            params=params,
        )
        r.raise_for_status()
        return r.json()

    def _post(self, table: str, data: dict, upsert: bool = False) -> requests.Response:
        headers = dict(self.headers)
        if upsert:
            headers["Prefer"] = "resolution=ignore-duplicates,return=minimal"
        return requests.post(f"{self.base}/{table}", headers=headers, json=data)

    def _patch(self, table: str, match: dict, data: dict) -> requests.Response:
        params = {k: f"eq.{v}" for k, v in match.items()}
        return requests.patch(
            f"{self.base}/{table}", headers=self.headers, params=params, json=data
        )

    # ── Public interface ────────────────────────────────────────────────────────

    def already_attempted(self, expr_hash: str) -> bool:
        """True if this hash is already in the attempted table."""
        rows = self._get("attempted", {"hash": f"eq.{expr_hash}", "select": "hash"})
        return len(rows) > 0

    def claim_attempt(self, expr_hash: str, expression: str, settings: dict, bot_id: str) -> bool:
        """
        Atomically insert into attempted.
        Returns True if we got the slot, False if someone else already has it.
        Uses ON CONFLICT DO NOTHING via Prefer header.
        """
        r = self._post(
            "attempted",
            {
                "hash": expr_hash,
                "expression": expression,
                "settings": json.dumps(settings),
                "bot_id": bot_id,
            },
            upsert=True,
        )
        if r.status_code in (200, 201):
            return True
        if r.status_code == 409:
            return False  # conflict — already claimed
        logger.warning(f"claim_attempt unexpected status {r.status_code}: {r.text[:200]}")
        return False

    def log_result(self, expr_hash: str, stats: dict, passed: bool):
        """Record simulation outcome."""
        r = self._post(
            "results",
            {
                "hash": expr_hash,
                "sharpe": stats.get("sharpe"),
                "fitness": stats.get("fitness"),
                "turnover": stats.get("turnover"),
                "margin": stats.get("margin"),
                "returns": stats.get("returns"),
                "drawdown": stats.get("drawdown"),
                "sim_id": stats.get("sim_id"),
                "passed": passed,
            },
            upsert=True,
        )
        if r.status_code not in (200, 201):
            logger.warning(f"log_result failed: {r.status_code} {r.text[:200]}")

    def log_submission(self, expr_hash: str, wq_alpha_id: str, bot_id: str):
        """Record that a passing alpha was submitted."""
        self._post(
            "submitted",
            {"hash": expr_hash, "wq_alpha_id": wq_alpha_id, "bot_id": bot_id},
            upsert=True,
        )

    def get_passing_count(self) -> int:
        """How many alphas have passed thresholds so far across all bots."""
        rows = self._get("results", {"passed": "eq.true", "select": "hash"})
        return len(rows)

    def get_attempted_count(self) -> int:
        rows = self._get("attempted", {"select": "hash"})
        return len(rows)

    def setup_tables_sql(self) -> str:
        """Returns the SQL to paste into Supabase SQL editor."""
        return """
-- Run this once in Supabase SQL editor

CREATE TABLE IF NOT EXISTS attempted (
    hash        TEXT PRIMARY KEY,
    expression  TEXT NOT NULL,
    settings    JSONB,
    bot_id      TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS results (
    hash        TEXT PRIMARY KEY REFERENCES attempted(hash),
    sharpe      REAL,
    fitness     REAL,
    turnover    REAL,
    margin      REAL,
    returns     REAL,
    drawdown    REAL,
    sim_id      TEXT,
    passed      BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS submitted (
    hash        TEXT PRIMARY KEY REFERENCES results(hash),
    wq_alpha_id TEXT,
    bot_id      TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Allow all operations (adjust for prod — add RLS if needed)
ALTER TABLE attempted ENABLE ROW LEVEL SECURITY;
ALTER TABLE results    ENABLE ROW LEVEL SECURITY;
ALTER TABLE submitted  ENABLE ROW LEVEL SECURITY;

CREATE POLICY "allow_all_attempted" ON attempted FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "allow_all_results"   ON results   FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "allow_all_submitted" ON submitted  FOR ALL USING (true) WITH CHECK (true);
        """.strip()
