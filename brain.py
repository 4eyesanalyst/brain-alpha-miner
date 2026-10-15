"""Minimal WorldQuant BRAIN API client: simulate Fast Expressions and report IS stats + checks.

Usage:  python brain.py ideas/batch1.json
        ideas file = [{"expr": "...", "settings": {...overrides}}, ...]
Credentials come from the BRAIN_EMAIL / BRAIN_PASSWORD environment variables
(falling back to the Windows user registry, so a restart isn't needed after setting them).
"""
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import requests

API = "https://api.worldquantbrain.com"
CONCURRENCY = 3
SIM_TIMEOUT = 30 * 60   # give up on a simulation that hasn't finished in 30 minutes
RESULTS = Path(__file__).parent / "results" / "simulations.jsonl"

DEFAULT_SETTINGS = {
    "instrumentType": "EQUITY", "region": "USA", "universe": "TOP3000", "delay": 1,
    "decay": 0, "neutralization": "SUBINDUSTRY", "truncation": 0.08, "pasteurization": "ON",
    "unitHandling": "VERIFY", "nanHandling": "OFF", "language": "FASTEXPR", "visualization": False,
}

_write_lock = threading.Lock()


def _credential(name):
    value = os.environ.get(name)
    if not value and sys.platform == "win32":
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value = winreg.QueryValueEx(key, name)[0]
    # Values piped in from Windows PowerShell can carry a byte-order mark and a trailing newline.
    value = (value or "").strip().lstrip("﻿").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


class Brain:
    def __init__(self):
        self.session = requests.Session()
        self.login()

    def login(self):
        res = self.session.post(f"{API}/authentication",
                                auth=(_credential("BRAIN_EMAIL"), _credential("BRAIN_PASSWORD")))
        if res.status_code == 401 and res.headers.get("WWW-Authenticate") == "persona":
            # BRAIN wants a one-off identity (biometric) check, e.g. when logging in from a new location.
            # The link belongs to this account, so it goes to Telegram, not into a (public) CI log.
            import notify
            link = f"{API}{res.headers.get('Location', '')}"
            sent = notify.send(f"🔴 BRAIN needs a one-off identity check before the miner can log in.\n"
                               f"Open this link, complete it, then re-run the workflow:\n{link}")
            public = os.environ.get("GITHUB_ACTIONS") == "true"
            raise RuntimeError("login needs identity verification: "
                               + ("link sent via Telegram" if public and sent else
                                  "check Telegram or run the miner locally to see the link" if public else
                                  f"open {link} in a browser, complete it, then rerun"))
        if res.status_code != 201:
            raise RuntimeError(f"login failed: HTTP {res.status_code} {res.text}")

    def request(self, method, url, **kwargs):
        url = url if url.startswith("http") else API + url
        for attempt in range(20):
            try:
                res = self.session.request(method, url, timeout=120, **kwargs)
            except (requests.ConnectionError, requests.Timeout):
                # BRAIN occasionally drops idle connections; back off and retry rather than lose the job.
                time.sleep(min(10 * (attempt + 1), 120))
                continue
            if res.status_code == 401 and attempt < 2:
                self.login()
                continue
            if res.status_code == 429:
                time.sleep(float(res.headers.get("Retry-After", 15)))
                continue
            return res
        raise requests.ConnectionError(f"gave up on {method} {url} after repeated failures")

    def simulate(self, expr, overrides=None):
        settings = {**DEFAULT_SETTINGS, **(overrides or {})}
        result = {"expr": expr, "settings": settings}
        res = self.request("POST", "/simulations", json={"type": "REGULAR", "settings": settings, "regular": expr})
        if res.status_code != 201:
            return {**result, "error": f"submit HTTP {res.status_code}: {res.text}"}
        progress = res.headers["Location"]

        deadline = time.time() + SIM_TIMEOUT
        while True:
            r = self.request("GET", progress)
            retry = float(r.headers.get("Retry-After", 0))
            sim = r.json()
            if not retry:
                break
            if time.time() > deadline:
                return {**result, "error": f"simulation still running after {SIM_TIMEOUT // 60} min; skipped"}
            time.sleep(retry)
        if sim.get("status") != "COMPLETE" or not sim.get("alpha"):
            return {**result, "error": sim.get("message") or sim.get("status")}

        alpha = self.request("GET", f"/alphas/{sim['alpha']}").json()
        stats = alpha.get("is") or {}
        return {
            **result, "alphaId": sim["alpha"],
            **{k: stats.get(k) for k in ("sharpe", "fitness", "turnover", "returns", "drawdown",
                                         "margin", "longCount", "shortCount")},
            "checks": [{k: c.get(k) for k in ("name", "result", "value", "limit")} for c in stats.get("checks", [])],
        }

    def _poll(self, url, timeout=600):
        """GET a resource BRAIN computes on demand, waiting while it says Retry-After (at most `timeout` s)."""
        deadline = time.time() + timeout
        while True:
            r = self.request("GET", url)
            retry = float(r.headers.get("Retry-After", 0))
            if not retry and r.text:
                return r.json()
            if time.time() > deadline:
                raise requests.Timeout(f"{url} not ready after {timeout} s")
            time.sleep(retry or 5)

    def daily_pnl(self, alpha_id):
        """{date: daily PnL} — the API returns cumulative PnL, so difference it."""
        records = self._poll(f"/alphas/{alpha_id}/recordsets/pnl").get("records", [])
        out, prev = {}, 0.0
        for date, cum in records:
            out[date] = cum - prev
            prev = cum
        return out

    def datasets(self, region="USA", delay=1, universe="TOP3000"):
        """Every dataset this account can use for the given region/delay/universe."""
        found, offset = [], 0
        while True:
            page = self.request("GET", "/data-sets", params={
                "instrumentType": "EQUITY", "region": region, "delay": delay, "universe": universe,
                "limit": 50, "offset": offset}).json()
            found += page.get("results", [])
            offset += 50
            if offset >= page.get("count", 0):
                return found

    def data_fields(self, dataset_id, region="USA", delay=1, universe="TOP3000"):
        fields, offset = [], 0
        while True:
            page = self.request("GET", "/data-fields", params={
                "instrumentType": "EQUITY", "region": region, "delay": delay, "universe": universe,
                "dataset.id": dataset_id, "limit": 50, "offset": offset}).json()
            fields += page.get("results", [])
            offset += 50
            if offset >= page.get("count", 0):
                return fields

    def label(self, alpha_id, tags, favorite=False):
        """Name the alpha after its ID (BRAIN's search looks at names, not IDs), tag it and colour it green."""
        body = {"name": alpha_id, "tags": tags, "color": "GREEN"}
        if favorite:
            body["favorite"] = True
        return self.request("PATCH", f"/alphas/{alpha_id}", json=body).ok

    def submit(self, alpha_id, timeout=900):
        """Submit an alpha (permanent). BRAIN re-runs its checks; returns (submitted?, detail)."""
        r = self.request("POST", f"/alphas/{alpha_id}/submit")
        if r.status_code >= 400:
            return False, f"HTTP {r.status_code}: {r.text[:300]}"
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self.request("GET", f"/alphas/{alpha_id}").json().get("status")
            if status == "ACTIVE":
                return True, status
            time.sleep(float(r.headers.get("Retry-After", 0)) or 10)
        return False, f"still {status} after {timeout // 60} min"

    def submissions_on(self, day):
        """How many alphas the account submitted on a given BRAIN day (YYYY-MM-DD, New York time)."""
        records = self.request("GET", "/users/self/activities/submissions").json().get("records", {}).get("records", [])
        return sum(n for d, n in records if d == day)

    def submitted_alphas(self):
        return self.request("GET", "/users/self/alphas", params={"status": "ACTIVE", "limit": 100}).json().get("results", [])

    def check(self, alpha_id):
        """Run the full pre-submission check (incl. self/prod correlation) — same as 'Check Submission'."""
        return self._poll(f"/alphas/{alpha_id}/check", timeout=900)


def summarize(r):
    if r.get("error"):
        return f"ERROR  {r['expr']}  -> {r['error']}"
    fails = [c["name"] for c in r["checks"] if c["result"] == "FAIL"]
    pending = [c["name"] for c in r["checks"] if c["result"] == "PENDING"]
    f = lambda x, scale=1, d=2: "  -  " if x is None else f"{x * scale:.{d}f}"
    s = r["settings"]
    line = (f"{'FAIL' if fails else 'PASS'} S={f(r['sharpe'])} F={f(r['fitness'])} "
            f"TO={f(r['turnover'], 100, 1)}% R={f(r['returns'], 100, 1)}% DD={f(r['drawdown'], 100, 1)}% "
            f"| {s['neutralization']}/d{s['decay']}/t{s['truncation']}{'/nan' if s['nanHandling'] == 'ON' else ''} "
            f"| {r['expr']} | {r['alphaId']}")
    if fails:
        line += f"  FAILS: {','.join(fails)}"
    if pending:
        line += f"  PENDING: {','.join(pending)}"
    return line


def run_all(ideas):
    brain = Brain()
    RESULTS.parent.mkdir(parents=True, exist_ok=True)

    def run(idea):
        try:
            r = brain.simulate(idea["expr"], idea.get("settings"))
        except Exception as e:  # keep the batch going if one idea blows up
            r = {"expr": idea["expr"], "settings": idea.get("settings"), "error": str(e)}
        with _write_lock:
            with RESULTS.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({"time": datetime.now().isoformat(), **r}) + "\n")
            print(summarize(r), flush=True)
        return r

    with ThreadPoolExecutor(CONCURRENCY) as pool:
        return list(pool.map(run, ideas))


if __name__ == "__main__":
    if sys.argv[1] == "check":  # python brain.py check <alphaId> [<alphaId> ...]
        brain = Brain()
        for alpha_id in sys.argv[2:]:
            checks = brain.check(alpha_id).get("is", {}).get("checks", [])
            print(f"== {alpha_id}")
            for c in checks:
                print(f"   {c.get('result'):8} {c.get('name'):32} value={c.get('value')} limit={c.get('limit')}")
    else:
        run_all(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")))
