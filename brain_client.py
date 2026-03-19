"""
WorldQuant Brain API client.
Handles auth, simulation submission, polling, and alpha submission.
"""

import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# Default simulation settings — override per run if needed
DEFAULT_SETTINGS = {
    "instrumentType": "EQUITY",
    "region": "USA",
    "universe": "TOP3000",
    "delay": 1,
    "decay": 0,
    "neutralization": "SUBINDUSTRY",
    "truncation": 0.08,
    "pasteurization": "ON",
    "unitHandling": "VERIFY",
    "nanHandling": "OFF",
    "language": "FASTEXPR",
    "visualization": False,
}

# Minimum thresholds to bother submitting
PASS_THRESHOLDS = {
    "sharpe": 1.25,
    "fitness": 1.0,
    "turnover_min": 0.01,
    "turnover_max": 0.70,
    "margin_min": 0.0,
}


class BrainClient:
    BASE = "https://api.worldquantbrain.com"

    def __init__(self, username: str, password: str):
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._authenticate(username, password)

    def _authenticate(self, username: str, password: str):
        self.session.auth = (username, password)
        r = self.session.post(f"{self.BASE}/authentication")
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Auth failed: {r.status_code} {r.text}")
        logger.info("Authenticated with WQ Brain")

    def simulate(self, expression: str, settings: Optional[dict] = None) -> dict:
        cfg = {**DEFAULT_SETTINGS, **(settings or {})}
        payload = {
            "type": "REGULAR",
            "settings": cfg,
            "regular": expression,
        }
        r = self.session.post(f"{self.BASE}/simulations", json=payload)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited — sleeping {wait}s")
            time.sleep(wait)
            return self.simulate(expression, settings)
        if r.status_code not in (200, 201):
            logger.warning(f"Simulation submit failed: {r.status_code} {r.text[:200]}")
            return {"error": r.text, "status": "SUBMIT_FAILED"}

        progress_url = r.headers.get("location")
        if not progress_url:
            return {"error": "No location header", "status": "SUBMIT_FAILED"}

        result = self._poll(progress_url)

        # Fetch alpha stats if simulation completed
        alpha_id = result.get("alpha")
        if alpha_id and result.get("status") in ("COMPLETE", "WARNING"):
            stats = self.get_alpha_stats(alpha_id)
            result["is"] = stats

        return result

    def get_alpha_stats(self, alpha_id: str) -> dict:
        r = self.session.get(f"{self.BASE}/alphas/{alpha_id}")
        if r.status_code != 200:
            return {}
        data = r.json()
        is_stats = data.get("is", {})
        return is_stats

    def _poll(self, url: str, max_wait: int = 300) -> dict:
        start = time.time()
        while time.time() - start < max_wait:
            try:
                r = self.session.get(url)
                if r.status_code != 200:
                    time.sleep(5)
                    continue
                data = r.json()
                status = data.get("status", "")
                if status not in ("RUNNING", "PENDING", "QUEUED", ""):
                    return data
            except Exception as e:
                logger.warning(f"Poll connection error, retrying: {e}")
            time.sleep(4)
        return {"status": "TIMEOUT"}

    def submit_alpha(self, simulation_result: dict) -> dict:
        """Submit a passing simulation as a saved alpha."""
        alpha_id = simulation_result.get("id")
        if not alpha_id:
            return {"error": "No simulation ID to submit"}
        r = self.session.patch(
            f"{self.BASE}/simulations/{alpha_id}",
            json={"stage": "ALPHA"},
        )
        if r.status_code not in (200, 201):
            logger.warning(f"Alpha submit failed: {r.status_code} {r.text[:200]}")
            return {"error": r.text}
        logger.info(f"Alpha submitted: {alpha_id}")
        return r.json()

    @staticmethod
    def passes_thresholds(result: dict) -> bool:
        """Check if a simulation result meets minimum quality bars."""
        if result.get("status") in ("ERROR", "SUBMIT_FAILED", "TIMEOUT"):
            return False
        stats = result.get("is", {})  # in-sample stats
        if not stats:
            return False
        sharpe = stats.get("sharpe", 0)
        fitness = stats.get("fitness", 0)
        turnover = stats.get("turnover", 0)
        margin = stats.get("margin", 0)
        return (
            sharpe >= PASS_THRESHOLDS["sharpe"]
            and fitness >= PASS_THRESHOLDS["fitness"]
            and PASS_THRESHOLDS["turnover_min"] <= turnover <= PASS_THRESHOLDS["turnover_max"]
            and margin >= PASS_THRESHOLDS["margin_min"]
        )

    @staticmethod
    def extract_stats(result: dict) -> dict:
        """Pull key stats out of a simulation result dict."""
        stats = result.get("is", {})
        return {
            "sharpe": stats.get("sharpe"),
            "fitness": stats.get("fitness"),
            "turnover": stats.get("turnover"),
            "margin": stats.get("margin"),
            "returns": stats.get("returns"),
            "drawdown": stats.get("drawdown"),
            "sim_id": result.get("id"),
        }
