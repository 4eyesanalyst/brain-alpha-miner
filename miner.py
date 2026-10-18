"""Alpha miner for WorldQuant BRAIN — searches every dataset on the account for submittable alphas.

Pipeline (see README.md for the full explanation):
  1. Discover every dataset/field the account can use (USA, Delay 1, TOP3000).
  2. Stage 1 — screen each field with a few proven idea templates.
  3. Stage 2 — ideas with |Sharpe| >= 1 get a "family" of tuning variants (window x decay x neutralization).
  4. When a family is finished, its best variant must pass the accuracy gates:
       every BRAIN IS check, Fitness >= 1, parameter stability, year-by-year consistency,
       PnL correlation < 0.7 with submitted alphas and with other winners, and the official
       pre-submission check. Survivors go to miner/ready.json (best first).

    python miner.py          mine (forever, or for MINER_MINUTES minutes then save and exit)
    python miner.py ready    re-check the ready list against your latest submissions and print it

All progress lives in miner/ so a run can stop at any time and the next run resumes.
"""
import hashlib
import itertools
import json
import math
import os
import re
import statistics
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

import notify
from brain import Brain, CONCURRENCY, DEFAULT_SETTINGS, summarize

# ---- knobs (environment variables override the defaults) ------------------------------------
MINUTES = int(os.environ.get("MINER_MINUTES", 0))                     # 0 = run forever
STOP_GRACE = 8 * 60        # after time is up, wait at most this long for in-flight work
ALPHA_URL = "https://platform.worldquantbrain.com/alpha/{}"
MAX_SIMS_PER_DAY = int(os.environ.get("MINER_MAX_SIMS_PER_DAY", 600))  # BRAIN asks for low-intensity API use
MIN_FITNESS = 1.0          # submission cutoff for Delay 1
PROMISING_SHARPE = 1.0     # stage-1 |Sharpe| that earns a tuning family
MIN_STABILITY = 0.34       # share of a family's variants that must reach Sharpe 1.0 (guards against lucky parameters)
MAX_BAD_YEARS = 1          # at most this many losing years in the in-sample period
MIN_RECENT_SHARPE = 0.5    # the last ~2 years must still be clearly profitable
MAX_CORRELATION = 0.7      # same cutoff BRAIN uses for self-correlation
REPAIRABLE = {"CONCENTRATED_WEIGHT"}  # failures worth a repair attempt when everything else is strong
SEND_MAX_CORRELATION = 0.6  # above this PnL correlation an alpha counts as a near-copy
# "all": every alpha passing BRAIN's official check goes to Telegram (near-copies flagged).
# "distinct": near-copies of submitted/already-sent alphas are held back.
TELEGRAM_MODE = os.environ.get("MINER_TELEGRAM", "all").lower()
MIN_FIELD_COVERAGE = 0.3
FIELDS_MAX_AGE_DAYS = 7

# Where to search: region/delay/universe combinations, interleaved. Smaller universes score more in the
# Challenge; Delay 0 is a separate pool of alphas (stricter cutoffs: Sharpe > 2, Fitness > 1.3).
SCOPES = [dict(zip(("region", "delay", "universe"), (r, int(d), u)))
          for r, d, u in (s.strip().split("/") for s in os.environ.get(
              "MINER_SCOPES", "USA/1/TOP3000,USA/1/TOP1000,USA/0/TOP3000,USA/1/TOP500").split(","))]
SMALLER_UNIVERSES = ["TOP1000", "TOP500", "TOPSP500", "TOP200"]  # a TOP3000 winner is also tried on these


def scope_name(scope):
    return f"{scope['region']}/D{scope['delay']}/{scope['universe']}"


def scope_settings(scope):
    return {"region": scope["region"], "delay": scope["delay"], "universe": scope["universe"]}


DIR = Path(__file__).parent / "miner"
TRIED = DIR / "tried.jsonl"        # one line per simulation ever run (resume + dedupe)
FAMILIES = DIR / "families.json"   # tuning families still in progress
READY = DIR / "ready.json"         # alphas worth submitting, best first
FIELDS = DIR / "fields.json"       # cached field catalogue (USA/D1/TOP3000; other scopes get their own file)
SEEDS = DIR / "seeds.json"         # researched ideas, tried before the generic templates (kept encrypted)
STATE = DIR / "state.json"         # daily simulation counter
REPORT = DIR / "report.json"       # day of the last Telegram daily report
REPORTED = DIR / "reported.json"   # every alpha already run through the official check for Telegram
SENT = DIR / "sent.json"           # alphas sent to the user (fitness, idea code), to keep new messages distinct
NEW_READY = DIR / "new_ready.md"   # winners found in this run (used for notifications)
LOG = DIR / "miner.log"
LOCK = DIR / "miner.lock"

# ---- idea templates -----------------------------------------------------------------------
# {x} = the prepared field (see field_expr), {w} = lookback window in trading days.
# Passes run in order over every field: seeds first, then the change/timing shapes, then levels.
# tsrank went last on 2026-10-18: ~2,600 simulations of that one shape left new finds failing
# SELF_CORRELATION against the submitted alphas built from it.
TEMPLATES = {
    "seed":     {"expr": "{x}", "windows": [None], "pass": 0},   # researched ideas from miner/seeds.json
    "revision": {"expr": "group_rank(ts_delta({x}, {w})/(abs(ts_delay({x}, {w})) + 0.0001), industry)",
                 "windows": [22, 66, 126], "pass": 1, "categories": {"analyst", "fundamental", "model"}},
    "volnorm":  {"expr": "group_rank(ts_delta({x}, {w})/(ts_std_dev({x}, {w}) + 0.0001), subindustry)",
                 "windows": [22, 66, 252], "pass": 1},
    "avdiff":   {"expr": "group_neutralize(ts_av_diff({x}, {w}), subindustry)", "windows": [22, 66, 252], "pass": 1},
    # distance from the field's own linear trend (regression residual on a day counter)
    "detrend":  {"expr": "group_rank(ts_regression({x}, ts_step(1), {w}), industry)", "windows": [60, 120, 252],
                 "pass": 1},
    # how recently the field peaked: fresh highs in estimates/news/sentiment lead price
    "recency":  {"expr": "group_rank(-ts_arg_max({x}, {w}), industry)", "windows": [20, 60, 120], "pass": 1,
                 "categories": {"analyst", "news", "socialmedia", "option", "model", "pv"}},
    # only re-trade when volume confirms (the market is reacting), hold otherwise; also cuts turnover
    "volgate":  {"expr": "trade_when(volume > ts_mean(volume, 20), group_rank(ts_zscore({x}, {w}), industry), -1)",
                 "windows": [22, 66, 126], "pass": 1},
    "zscore":   {"expr": "ts_zscore({x}, {w})", "windows": [60, 126, 252], "pass": 2},
    "level":    {"expr": "group_rank({x}, subindustry)", "windows": [None], "pass": 2},
    "to_cap":   {"expr": "group_rank({x}/cap, subindustry)", "windows": [None], "pass": 2,
                 "categories": {"fundamental", "analyst"}},
    "tsrank":   {"expr": "group_rank(ts_rank({x}, {w}), industry)", "windows": [60, 120, 252], "pass": 3},
}
PASSES = (0, 1, 2, 3)

# What works differs by data type (BRAIN docs + community experience): the first neutralization is
# used for screening, both are tried when tuning; decay is the screening decay.
CATEGORY_SETTINGS = {
    "fundamental": {"neutralizations": ["SUBINDUSTRY", "INDUSTRY"], "decay": 2},
    "analyst":     {"neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decay": 2},
    "model":       {"neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decay": 2},
    "option":      {"neutralizations": ["SECTOR", "MARKET"], "decay": 4},
    "news":        {"neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decay": 6},
    "socialmedia": {"neutralizations": ["INDUSTRY", "SUBINDUSTRY"], "decay": 6},
    "pv":          {"neutralizations": ["INDUSTRY", "MARKET"], "decay": 10},
}
DEFAULT_CATEGORY = {"neutralizations": ["SUBINDUSTRY", "INDUSTRY"], "decay": 4}
BASE = {"nanHandling": "ON", "truncation": 0.08}

# Fields that never make signals: corporate-action bookkeeping and static relationship counts
# (turnover ~1% caps Fitness no matter the Sharpe).
TRAP_FIELDS = {"adjfactor", "split", "dividend"}
TRAP_PATTERN = re.compile(r"count|graphrank|revere")
PRICE_LEVEL_PATTERN = re.compile(r"breakeven|forward_price")  # option prices: only meaningful relative to close
SUM_VECTOR_PATTERN = re.compile(r"buzz|volume|count|num")      # vectors of counts are summed, scores averaged


def fields_path(scope):
    if (scope["region"], scope["delay"], scope["universe"]) == ("USA", 1, "TOP3000"):
        return FIELDS  # the original cache file
    return DIR / f"fields_{scope['region']}_D{scope['delay']}_{scope['universe']}.json"


def field_expr(f):
    """Turn a catalogue field into the expression the templates operate on (or None to skip it)."""
    fid = f["id"]
    if fid in TRAP_FIELDS or (f["dataset"].startswith("pv13") and TRAP_PATTERN.search(fid)):
        return None
    x = fid
    if f["type"] == "VECTOR":
        x = f"{'vec_sum' if SUM_VECTOR_PATTERN.search(fid) else 'vec_avg'}({fid})"
    if PRICE_LEVEL_PATTERN.search(fid):
        x = f"{x}/close"
    if f["dataset"] == "news12" and f["type"] == "VECTOR":
        x = f"ts_backfill({x}, 504)"   # sparse news events: long backfill keeps coverage up
    elif f["coverage"] < 0.8:
        x = f"ts_backfill({x}, {252 if f['category'] == 'fundamental' else 66})"
    return x


def tune_decays(turnover):
    """WorldQuant's own rule of thumb: more decay for fast signals; below 12.5% turnover decay can't help Fitness."""
    if turnover is None:
        return [3, 8]
    if turnover > 0.5:
        return [12, 20]
    if turnover > 0.3:
        return [6, 12]
    if turnover > 0.125:
        return [2, 6]
    return [0, 3]


def neutralizations_for(category, first=None):
    pair = list(CATEGORY_SETTINGS.get(category, DEFAULT_CATEGORY)["neutralizations"])
    if first and first not in pair:
        pair = [first, "MARKET" if first == "SECTOR" else "INDUSTRY" if first != "INDUSTRY" else "SUBINDUSTRY"]
    return pair

_lock = threading.Lock()


# In public GitHub Actions logs, print only lines that contain no alpha formulas; the full log
# goes to miner/miner.log, which is stored encrypted (see vault.py).
PRIVATE_CONSOLE = os.environ.get("GITHUB_ACTIONS") == "true" or os.environ.get("MINER_PRIVATE_LOGS") == "1"


def log(msg, public=False):
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    if public or not PRIVATE_CONSOLE:
        print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def full(settings):
    return {**DEFAULT_SETTINGS, **settings}


def key(expr, settings):
    return hashlib.sha1((expr + json.dumps(full(settings), sort_keys=True)).encode()).hexdigest()


def fails(r):
    if "fails" in r:
        return r["fails"]
    return [c["name"] for c in r.get("checks", []) if c.get("result") == "FAIL"]


def passes_is(r):
    """Passed every in-sample check BRAIN ran during the simulation (self-correlation is checked separately)."""
    return (not r.get("error") and r.get("alphaId") and not fails(r)
            and (r.get("fitness") or 0) >= MIN_FITNESS and (r.get("sharpe") or 0) > 0)


def idea_code(expr):
    """Same code for variants of one idea (different windows, sign, gap-filling repair) so the user can spot near-copies."""
    core = expr.strip()
    if core.startswith("ts_backfill(") and core.endswith(", 5)"):
        core = core[len("ts_backfill("):-len(", 5)")]
    if core.startswith("-(") and core.endswith(")"):
        core = core[2:-1]
    core = re.sub(r"\d+(\.\d+)?", "", core.lstrip("-")).replace(" ", "")
    return hashlib.sha1(core.encode()).hexdigest()[:5].upper()


def corr(a, b):
    dates = [d for d in a if d in b]
    if len(dates) < 100:
        return 0.0
    xs, ys = [a[d] for d in dates], [b[d] for d in dates]
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    vy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return cov / (vx * vy) if vx and vy else 0.0


def yearly_sharpe(pnl):
    by_year = defaultdict(list)
    for d, v in pnl.items():
        by_year[d[:4]].append(v)
    out = {}
    for year, vals in sorted(by_year.items()):
        if len(vals) >= 60:  # skip stub years at the edges of the simulation window
            sd = statistics.pstdev(vals)
            out[year] = round(statistics.mean(vals) / sd * math.sqrt(252), 2) if sd else 0.0
    return out


def recent_sharpe(pnl, days=504):
    """Sharpe of the last ~2 years: BRAIN also checks recent performance, and it's the best hint of OS behaviour."""
    vals = [pnl[d] for d in sorted(pnl)][-days:]
    sd = statistics.pstdev(vals) if len(vals) > 60 else 0
    return statistics.mean(vals) / sd * math.sqrt(252) if sd else 0.0


def write_json(path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1), encoding="utf-8")
    tmp.replace(path)


class Miner:
    def __init__(self):
        DIR.mkdir(exist_ok=True)
        self.brain = Brain()
        self.tried = {}
        if TRIED.exists():
            for line in TRIED.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    r = json.loads(line)
                    self.tried[key(r["expr"], r["settings"])] = r
        self.ready = json.loads(READY.read_text(encoding="utf-8")) if READY.exists() else []
        self.families = json.loads(FAMILIES.read_text(encoding="utf-8")) if FAMILIES.exists() else {}
        state = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}
        self.day, self.sims_today = state.get("day", str(date.today())), state.get("sims", 0)
        self.priority = deque()          # tuning jobs and retries run before new stage-1 ideas
        self.stage1 = self._stage1_jobs()
        self.stop = threading.Event()
        self.reference_pnls, self.ready_pnls = {}, {}
        self.run_sims, self.run_winners = 0, 0
        reported = json.loads(REPORTED.read_text(encoding="utf-8")) if REPORTED.exists() else {}
        self.reported = {k: v for k, v in reported.items() if v != "checking"}  # retry checks cut off by a stop
        # Alphas already sent to the user (to keep later messages distinct from them)
        if SENT.exists():
            self.sent = json.loads(SENT.read_text(encoding="utf-8"))
        else:
            by_id = {r.get("alphaId"): r for r in self.tried.values()}
            self.sent = {aid: {"fitness": by_id[aid]["fitness"], "sharpe": by_id[aid]["sharpe"],
                               "code": idea_code(by_id[aid]["expr"])}
                         for aid, v in self.reported.items() if v in ("pass", "passed") and aid in by_id}
        self.sent_pnls = {}

    # ---- inputs ---------------------------------------------------------------------------
    def load_fields(self, scope, force=False):
        path = fields_path(scope)
        if not force and path.exists() and time.time() - path.stat().st_mtime < FIELDS_MAX_AGE_DAYS * 86400:
            return json.loads(path.read_text(encoding="utf-8"))
        per_dataset = {}
        for ds in self.brain.datasets(**scope):
            try:
                fs = [f for f in self.brain.data_fields(ds["id"], **scope)
                      if f["type"] in ("MATRIX", "VECTOR") and (f.get("coverage") or 0) >= MIN_FIELD_COVERAGE]
            except requests.RequestException as e:
                log(f"fields: {scope_name(scope)} {ds['id']} failed: {e}", public=True)
                continue
            # Least-crowded fields first: fewer existing alphas means a better chance of low correlation.
            per_dataset[ds["id"]] = sorted(fs, key=lambda f: f.get("alphaCount") or 0)
            log(f"fields: {scope_name(scope)} {ds['id']:16} {len(fs):5} usable  ({ds['name']})", public=True)
        # Round-robin across datasets so every dataset gets explored early.
        ordered = [f for group in itertools.zip_longest(*per_dataset.values()) for f in group if f]
        slim = [{"id": f["id"], "type": f["type"], "coverage": f.get("coverage") or 0,
                 "dataset": f["dataset"]["id"], "category": (f.get("category") or {}).get("id", "")}
                for f in ordered]
        write_json(path, slim)
        return slim

    def _scope_jobs(self, scope, p, seeds):
        """Stage-1 jobs of one template pass in one scope (region/delay/universe)."""
        if p == 0:
            if scope["delay"] != 1:
                return  # the researched seeds are Delay-1 ideas
            candidates = [({"dataset": s.get("dataset", "seed"), "category": s.get("category", "")}, s["expr"],
                           {k: s[k] for k in ("neutralization", "decay", "truncation") if k in s}) for s in seeds]
        else:
            candidates = []
            for f in self.load_fields(scope):
                x = field_expr(f)
                if x:
                    cat = CATEGORY_SETTINGS.get(f["category"], DEFAULT_CATEGORY)
                    candidates.append((f, x, {"neutralization": cat["neutralizations"][0], "decay": cat["decay"]}))
        for f, x, settings in candidates:
            for name, t in TEMPLATES.items():
                if t["pass"] != p or ("categories" in t and f["category"] not in t["categories"]):
                    continue
                yield {"stage": 1, "template": name, "x": x, "dataset": f["dataset"], "category": f["category"],
                       "expr": t["expr"].format(x=x, w=t["windows"][len(t["windows"]) // 2]),
                       "settings": {**BASE, **settings, **scope_settings(scope)}}

    def _stage1_jobs(self):
        """Endless stream of untried stage-1 jobs: pass by pass, interleaving every scope, then refresh."""
        while True:
            new = 0
            seeds = json.loads(SEEDS.read_text(encoding="utf-8")) if SEEDS.exists() else []
            for p in PASSES:
                streams = [self._scope_jobs(scope, p, seeds) for scope in SCOPES]
                for group in itertools.zip_longest(*streams):
                    for job in group:
                        if job and key(job["expr"], job["settings"]) not in self.tried:
                            new += 1
                            yield job
            if not new:
                log("every field and template has been tried; checking for new data fields every 6 hours", public=True)
                until = time.time() + 6 * 3600
                while time.time() < until:
                    yield None  # tells the worker to idle (never sleep here: the caller holds the lock)
                for scope in SCOPES:
                    self.load_fields(scope, force=True)

    def refresh_references(self):
        submitted = self.brain.submitted_alphas()
        for a in submitted:
            if a["id"] not in self.reference_pnls:
                self.reference_pnls[a["id"]] = self.brain.daily_pnl(a["id"])
        with _lock:
            # Anything submitted since it was found leaves the ready list.
            self.ready = [r for r in self.ready if r["alphaId"] not in self.reference_pnls]
            write_json(READY, self.ready)
        for r in self.ready:
            if r["alphaId"] not in self.ready_pnls:
                self.ready_pnls[r["alphaId"]] = self.brain.daily_pnl(r["alphaId"])

    # ---- job flow -------------------------------------------------------------------------
    def next_job(self):
        with _lock:
            if self.priority:
                return self.priority.popleft()
            return next(self.stage1)

    def take_budget(self):
        with _lock:
            today = str(date.today())
            if today != self.day:
                self.day, self.sims_today = today, 0
            if self.sims_today >= MAX_SIMS_PER_DAY:
                return False
            self.sims_today += 1
            write_json(STATE, {"day": self.day, "sims": self.sims_today})
            return True

    def record(self, job, r):
        slim = {k: r.get(k) for k in ("expr", "settings", "alphaId", "sharpe", "fitness", "turnover",
                                      "returns", "drawdown", "error")}
        slim |= {"time": datetime.now().isoformat(timespec="seconds"), "stage": job["stage"],
                 "template": job["template"], "dataset": job.get("dataset"), "fails": fails(r)}
        with _lock:
            self.tried[key(job["expr"], job["settings"])] = slim
            with TRIED.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(slim) + "\n")
            self.run_sims += 1
        return slim

    def open_family(self, job, r):
        """Stage 2: a grid of variants around a promising idea, evaluated together once all are done."""
        sign = 1 if r["sharpe"] > 0 else -1
        t = TEMPLATES[job["template"]]
        jobs = []
        if sign > 0:
            jobs.append({k: job[k] for k in ("expr", "settings")})  # the stage-1 result itself competes too
        neutralizations = neutralizations_for(job.get("category"), job["settings"]["neutralization"])
        # Variants stay in the idea's own scope (region/delay/universe) and keep its truncation.
        keep = {k: job["settings"][k] for k in ("region", "delay", "universe", "truncation") if k in job["settings"]}
        for w, d, n in itertools.product(t["windows"], tune_decays(r.get("turnover")), neutralizations):
            expr = t["expr"].format(x=job["x"], w=w)
            jobs.append({"expr": f"-({expr})" if sign < 0 else expr,
                         "settings": {**BASE, **keep, "neutralization": n, "decay": d}})
        scope_tag = "" if not {"region", "delay", "universe"} & keep.keys() else \
            "|" + "/".join(str(keep.get(k, "")) for k in ("region", "delay", "universe"))
        fid = hashlib.sha1(f"{job['template']}|{job['x']}|{sign}{scope_tag}".encode()).hexdigest()[:12]
        with _lock:
            if fid in self.families:
                return
            self.families[fid] = {"template": job["template"], "x": job["x"], "dataset": job.get("dataset"),
                                  "category": job.get("category"), "sign": sign, "jobs": jobs}
            write_json(FAMILIES, self.families)
        log(f"  promising (S={r['sharpe']:.2f}) -> tuning {len(jobs)} variants: {job['expr']}")
        self.schedule_family(fid)

    def schedule_family(self, fid):
        fam = self.families[fid]
        pending = [j for j in fam["jobs"] if key(j["expr"], j["settings"]) not in self.tried]
        with _lock:
            for j in pending:
                self.priority.append({"stage": 2, "template": fam["template"], "x": fam["x"],
                                      "dataset": fam.get("dataset"), "category": fam.get("category"), "fid": fid, **j})
        if not pending:
            self.finish_family(fid)

    def finish_family(self, fid):
        with _lock:
            fam = self.families.get(fid)
            if not fam or any(key(j["expr"], j["settings"]) not in self.tried for j in fam["jobs"]):
                return
            del self.families[fid]
            write_json(FAMILIES, self.families)
            results = [self.tried[key(j["expr"], j["settings"])] for j in fam["jobs"]]
        self.evaluate(fam, results)

    # ---- accuracy gates -------------------------------------------------------------------
    def evaluate(self, fam, results):
        ok = [r for r in results if not r.get("error") and r.get("sharpe") is not None]
        if not ok:
            return
        # A repair family inherits the stability of the family it came from.
        stability = fam.get("parent_stability", sum(r["sharpe"] >= PROMISING_SHARPE for r in ok) / len(ok))
        good = lambda r: (r["fitness"] or 0) >= MIN_FITNESS and r["sharpe"] > 0
        passing = sorted((r for r in ok if not fails(r) and good(r)), key=lambda r: -r["fitness"])
        best = max(ok, key=lambda r: r["fitness"] or -9)
        log(f"  family done: {fam['template']} {fam['x']} -> {len(passing)}/{len(ok)} pass, "
            f"stability {stability:.0%}, best F={best['fitness']}")
        if stability < MIN_STABILITY:
            if passing:
                log("  rejected: only a few parameter choices work (likely luck, would not hold up out-of-sample)")
            return
        for r in passing[:3]:
            if self.consider(r, stability):
                return
        near = [r for r in ok if good(r) and fails(r) and set(fails(r)) <= REPAIRABLE]
        if near and fam["template"] != "repair":
            self.open_repair(fam, max(near, key=lambda r: r["fitness"]), stability)

    def open_repair(self, fam, r, stability):
        """Strong signal that fails only on weight concentration: usually days with missing data push the book
        into the few stocks that do have values. Fill short gaps and/or cap single-stock weight lower."""
        filled = f"ts_backfill({r['expr']}, 5)"
        s = r["settings"]
        jobs = [{"expr": filled, "settings": {**s}},
                {"expr": filled, "settings": {**s, "truncation": 0.05}},
                {"expr": r["expr"], "settings": {**s, "truncation": 0.05}}]
        fid = hashlib.sha1(f"repair|{r['expr']}|{json.dumps(s, sort_keys=True)}".encode()).hexdigest()[:12]
        with _lock:
            if fid in self.families:
                return
            self.families[fid] = {"template": "repair", "x": r["expr"], "dataset": fam.get("dataset"),
                                  "category": fam.get("category"), "sign": 1, "parent_stability": stability,
                                  "jobs": jobs}
            write_json(FAMILIES, self.families)
        log(f"  near miss (only {fails(r)}) -> trying {len(jobs)} repairs: {r['expr']}")
        self.schedule_family(fid)

    def consider(self, r, stability):
        """Returns True once the idea is settled (accepted or already covered), False to try the next variant."""
        pnl = self.brain.daily_pnl(r["alphaId"])
        years = yearly_sharpe(pnl)
        bad_years = [y for y, s in years.items() if s <= 0]
        if len(years) < 3 or len(bad_years) > MAX_BAD_YEARS:
            log(f"  {r['alphaId']} rejected: losing years {bad_years} {years}")
            return False
        recent = recent_sharpe(pnl)
        if recent < MIN_RECENT_SHARPE:
            log(f"  {r['alphaId']} rejected: weak recent performance (last-2-year Sharpe {recent:.2f})")
            return False
        with _lock:
            max_corr = 0.0
            for sid, ref in self.reference_pnls.items():
                c = corr(pnl, ref)
                max_corr = max(max_corr, c)
                if c > MAX_CORRELATION:
                    log(f"  {r['alphaId']} rejected: PnL correlation {c:.2f} with your submitted alpha {sid}")
                    return True
            rivals = []
            for x in self.ready:
                c = corr(pnl, self.ready_pnls.get(x["alphaId"], {}))
                if c > MAX_CORRELATION:
                    rivals.append(x)
                else:
                    max_corr = max(max_corr, c)
            if any(x["fitness"] >= r["fitness"] for x in rivals):
                return True  # an equal-or-better version of this idea is already on the list
        checks = self.brain.check(r["alphaId"]).get("is", {}).get("checks", [])
        failed = [c["name"] for c in checks if c.get("result") == "FAIL"]
        if failed:
            log(f"  {r['alphaId']} failed the official pre-submission check: {failed}")
            return False
        entry = {k: r[k] for k in ("alphaId", "expr", "settings", "sharpe", "fitness", "turnover", "returns", "drawdown")}
        entry |= {"dataset": r.get("dataset"), "stability": round(stability, 2), "yearly_sharpe": years,
                  "recent_2y_sharpe": round(recent, 2),
                  "max_correlation": round(max_corr, 2), "found": datetime.now().isoformat(timespec="seconds")}
        with _lock:
            self.ready = [x for x in self.ready if x not in rivals] + [entry]
            for x in rivals:
                self.ready_pnls.pop(x["alphaId"], None)
            self.ready_pnls[r["alphaId"]] = pnl
            self.ready.sort(key=lambda x: -x["fitness"])
            write_json(READY, self.ready)
            self.run_winners += 1
            s = r["settings"]
            with NEW_READY.open("a", encoding="utf-8") as fh:  # posted publicly: IDs and stats only, no formula
                fh.write(f"### Alpha **{r['alphaId']}** ({r.get('dataset') or 'unknown'} data)\n"
                         f"- Sharpe {r['sharpe']:.2f} · Fitness {r['fitness']:.2f} · Turnover {r['turnover']*100:.1f}% · "
                         f"Returns {r['returns']*100:.1f}% · Drawdown {r['drawdown']*100:.1f}%\n"
                         f"- Settings: {s['region']} {s['universe']}, delay {s['delay']}, decay {s['decay']}, "
                         f"{s['neutralization']} neutralization, truncation {s['truncation']}, NaN handling {s['nanHandling']}\n"
                         f"- Stability {stability:.0%} · yearly Sharpe {years} · max correlation {max_corr:.2f}\n\n")
        log(f"*** READY: {r['alphaId']} S={r['sharpe']:.2f} F={r['fitness']:.2f} {r['expr']}"
            + (f" (replaces {len(rivals)} weaker correlated version)" if rivals else ""))
        if PRIVATE_CONSOLE:
            print(f"*** new alpha ready to submit: {r['alphaId']} Sharpe={r['sharpe']:.2f} Fitness={r['fitness']:.2f}", flush=True)
        if r["alphaId"] not in self.sent:
            return True  # not distinct enough to have been sent; don't recommend it either
        self.brain.label(r["alphaId"], ["passing", "recommended", f"idea-{idea_code(r['expr'])}"], favorite=True)
        notify.send(f"⭐ Recommended: {r['alphaId']} (idea {idea_code(r['expr'])})\n"
                    f"The best version of this idea, and it also clears the miner's extra quality gates: "
                    f"stable across settings ({stability:.0%}), at most one losing year, strong last 2 years "
                    f"(Sharpe {recent:.2f}), and distinct from your submitted alphas (max corr {max_corr:.2f}).\n"
                    f"Starred ⭐ in your BRAIN account. Open: {ALPHA_URL.format(r['alphaId'])}")
        return True

    # ---- Telegram: every alpha that passes BRAIN's checks -------------------------------
    def report_pass(self, r):
        """Run the official check on an alpha that passed the IS checks; if it passes, send it to Telegram."""
        aid = r["alphaId"]
        with _lock:
            if aid in self.reported:
                return
            self.reported[aid] = "checking"
        checks = self.brain.check(aid).get("is", {}).get("checks", [])
        failed = [c["name"] for c in checks if c.get("result") == "FAIL"]
        if failed:
            self._mark(aid, "fail:" + ",".join(failed))
            log(f"  {aid} passed the simulation checks but failed the official check: {failed}")
            return

        # How it relates to what the user already has. In "distinct" mode near-copies are held back
        # (BRAIN lets a near-copy pass if its Sharpe is 10% higher); in "all" mode they're sent with a warning.
        self.refresh_references()  # include anything submitted since the last refresh
        pnl = self.brain.daily_pnl(aid)
        corrs = {sid: corr(pnl, ref) for sid, ref in self.reference_pnls.items()}
        closest = max(corrs, key=corrs.get) if corrs else None
        copy_of_submitted = closest if closest and corrs[closest] >= SEND_MAX_CORRELATION else None
        if copy_of_submitted and TELEGRAM_MODE == "distinct":
            self._mark(aid, f"similar-to-submitted:{closest}")
            log(f"  {aid} not sent: {corrs[closest]:.2f} correlated with your submitted alpha {closest}")
            return
        replaces, sibling, max_pending = None, None, 0.0
        for sid, info in list(self.sent.items()):
            if sid in self.reference_pnls:
                continue  # already submitted: covered by the check above
            c = corr(pnl, self.sent_pnl(sid))
            if c >= SEND_MAX_CORRELATION:
                sibling = sibling or (sid, c, info["fitness"])
                if (r["fitness"] or 0) <= info["fitness"]:
                    if TELEGRAM_MODE == "distinct":
                        self._mark(aid, f"weaker-than-sent:{sid}")
                        log(f"  {aid} not sent: a better version ({sid}) was already sent")
                        return
                else:
                    replaces = replaces or sid
            else:
                max_pending = max(max_pending, c)

        code = idea_code(r["expr"])
        self.brain.label(aid, ["passing", f"idea-{code}"])
        with _lock:
            if replaces and TELEGRAM_MODE == "distinct":
                self.sent.pop(replaces, None)
            self.sent[aid] = {"fitness": r["fitness"], "sharpe": r["sharpe"], "code": code}
            self.sent_pnls[aid] = pnl
            write_json(SENT, self.sent)
        if replaces and TELEGRAM_MODE == "distinct":
            self.brain.request("PATCH", f"/alphas/{replaces}", json={"tags": ["passing", "superseded"]})
        self._mark(aid, "passed")

        if copy_of_submitted:
            header = (f"⚠️ Near-copy of your submitted {copy_of_submitted} (correlation {corrs[closest]:.2f}). "
                      f"It passes BRAIN's check now, but adds little: usually skip it.\n\n")
        elif replaces:
            header = f"🔁 Better version of {replaces} (sent earlier): submit only one of them, this one is stronger.\n\n"
        elif sibling:
            header = (f"🔂 Same idea as {sibling[0]} (sent earlier, correlation {sibling[1]:.2f}, "
                      f"Fitness {sibling[2]:.2f}): submit only one of them.\n\n")
        else:
            header = "🆕 New idea, different from everything you've submitted or been sent.\n\n"
        years, recent = yearly_sharpe(pnl), recent_sharpe(pnl)
        s = r["settings"]
        if s.get("universe", "TOP3000") != "TOP3000" and (replaces or sibling):
            header += (f"📉 This is the {s['universe']} version. The Challenge scores smaller universes higher, "
                       f"so prefer it over the TOP3000 one if the stats are similar.\n\n")
        years_txt = " ".join(f"{y[2:]}:{v:+.1f}" for y, v in years.items())
        log(f"  PASS sent to Telegram: {aid} {r['expr']}")
        notify.send(header
                    + f"✅ Passing alpha: {aid}\n"
                    f"Idea {code} · data: {r.get('dataset') or 'unknown'}\n"
                    f"Sharpe {r['sharpe']:.2f} · Fitness {r['fitness']:.2f} · Turnover {r['turnover']*100:.1f}%\n"
                    f"Returns {r['returns']*100:.1f}% · Drawdown {r['drawdown']*100:.1f}%\n"
                    f"Yearly Sharpe {years_txt} · last 2y {recent:.2f}\n"
                    f"Max correlation {corrs[closest] if closest else 0:.2f} with your submitted alphas"
                    + (f" ({closest})" if closest else "")
                    + f", {max_pending:.2f} with other alphas sent to you\n"
                    f"Settings: {s.get('region', 'USA')} {s.get('universe', 'TOP3000')} · Delay {s.get('delay', 1)} · "
                    f"{s['neutralization']}, decay {s['decay']}, truncation {s['truncation']}\n"
                    f"Open: {ALPHA_URL.format(aid)}\n"
                    f"Or in BRAIN: Alphas → Unsubmitted → search \"{aid}\"")

        # A TOP3000 winner may also work on smaller universes, which score higher: try it there next.
        if s.get("universe", "TOP3000") == "TOP3000" and s.get("delay", 1) == 1:
            with _lock:
                for u in SMALLER_UNIVERSES:
                    self.priority.append({"stage": 3, "template": "universe", "x": r["expr"],
                                          "dataset": r.get("dataset"), "category": r.get("category"),
                                          "expr": r["expr"], "settings": {**s, "universe": u}})

    def _mark(self, aid, status):
        with _lock:
            self.reported[aid] = status
            write_json(REPORTED, self.reported)

    def sent_pnl(self, aid):
        if aid not in self.sent_pnls:
            self.sent_pnls[aid] = self.brain.daily_pnl(aid)
        return self.sent_pnls[aid]

    def report_backlog(self):
        """Send passing alphas found before Telegram reporting existed (or cut off by a stop); label older passes."""
        for aid, status in list(self.reported.items()):
            if status == "pass":  # reported before labelling existed
                rec = next((r for r in self.tried.values() if r.get("alphaId") == aid), None)
                if rec and self.brain.label(aid, ["passing", f"idea-{idea_code(rec['expr'])}"]):
                    with _lock:
                        self.reported[aid] = "passed"
                        write_json(REPORTED, self.reported)
        backlog = [r for r in list(self.tried.values()) if passes_is(r) and r["alphaId"] not in self.reported]
        for r in sorted(backlog, key=lambda r: -(r["fitness"] or 0)):
            if self.stop.is_set():
                return
            try:
                self.report_pass(r)
            except requests.RequestException as e:
                log(f"backlog report failed for {r['alphaId']}: {e}", public=True)
                with _lock:
                    self.reported.pop(r["alphaId"], None)

    # ---- main loop ------------------------------------------------------------------------
    def worker(self):
        while not self.stop.is_set():
            job = None
            try:
                if not self.take_budget():
                    if MINUTES:
                        log("daily simulation budget used up; ending this run", public=True)
                        self.stop.set()
                        return
                    time.sleep(600)
                    continue
                job = self.next_job()
                if job is None or key(job["expr"], job["settings"]) in self.tried:
                    with _lock:
                        self.sims_today -= 1  # nothing was simulated
                    if job is None:
                        time.sleep(300)
                    elif job.get("fid"):
                        self.finish_family(job["fid"])
                    continue
                r = self.brain.simulate(job["expr"], job["settings"])
                log(f"[s{job['stage']}] {summarize(r)}")
                slim = self.record(job, r)
                if PRIVATE_CONSOLE:  # formula-free progress line for the public log
                    status = "error" if r.get("error") else ("pass" if not slim["fails"] else "fail")
                    print(f"{datetime.now():%H:%M:%S} sim #{self.run_sims} stage {job['stage']} {job.get('dataset', '')}: "
                          f"{status} Sharpe={r.get('sharpe')} Fitness={r.get('fitness')}", flush=True)
                if passes_is(slim):
                    self.report_pass(slim)
                if not r.get("error") and job["stage"] == 1 and abs(r["sharpe"] or 0) >= PROMISING_SHARPE:
                    self.open_family(job, r)
                if job.get("fid"):
                    self.finish_family(job["fid"])
            except requests.RequestException as e:
                # Network outage: keep the idea so it isn't lost, then wait for the connection to return.
                if job:
                    with _lock:
                        self.priority.appendleft(job)
                log(f"network problem, retrying in 2 min: {type(e).__name__}", public=True)
                time.sleep(120)
            except Exception as e:
                log("worker error:\n" + traceback.format_exc())
                log(f"worker error: {type(e).__name__} (details in the encrypted log)", public=True)
                time.sleep(60)

    def run(self):
        NEW_READY.unlink(missing_ok=True)
        log(f"miner started: {len(self.tried)} simulations already tried, {len(self.families)} families in progress, "
            f"{len(self.ready)} ready" + (f", running for {MINUTES} min" if MINUTES else ""), public=True)
        self.refresh_references()
        for fid in list(self.families):
            self.schedule_family(fid)
        deadline = time.time() + MINUTES * 60 if MINUTES else None
        threads = [threading.Thread(target=self.worker, daemon=True) for _ in range(CONCURRENCY)]
        for t in threads:
            t.start()
        threading.Thread(target=self.report_backlog, daemon=True).start()
        last_refresh = time.time()
        stopped_at = None
        while any(t.is_alive() for t in threads):
            time.sleep(15)
            if deadline and time.time() >= deadline and not self.stop.is_set():
                log("time is up; finishing the simulations in flight", public=True)
                self.stop.set()
            if self.stop.is_set():
                stopped_at = stopped_at or time.time()
                if time.time() - stopped_at > STOP_GRACE:
                    # Everything finished so far is already on disk; leave stragglers so the run can save in time.
                    log(f"wind-down took over {STOP_GRACE // 60} min; leaving the remaining work for the next run",
                        public=True)
                    break
            if time.time() - last_refresh > 3600:
                last_refresh = time.time()
                try:
                    self.refresh_references()
                except requests.RequestException as e:
                    log(f"refresh failed: {e}", public=True)
        self.daily_report()
        log(f"run finished: {self.run_sims} simulations, {self.run_winners} new alphas ready, "
            f"{len(self.ready)} on the ready list, {len(self.families)} families still tuning", public=True)


    def daily_report(self):
        """Once a day: a short Telegram heartbeat so silence never means 'did it stop?'."""
        today = str(date.today())
        if REPORT.exists() and json.loads(REPORT.read_text(encoding="utf-8")).get("day") == today:
            return
        cutoff = (datetime.now() - timedelta(days=1)).isoformat()
        last_day = [r for r in self.tried.values() if (r.get("time") or "") >= cutoff]
        promising = sum(abs(r.get("sharpe") or 0) >= PROMISING_SHARPE for r in last_day)
        waiting = ", ".join(r["alphaId"] for r in self.ready) or "none"
        if notify.send(f"⚪ Daily miner report\n"
                       f"Last 24h: {len(last_day)} simulations, {promising} promising\n"
                       f"Ready to submit: {waiting}\n"
                       f"Ideas being tuned: {len(self.families)} · total simulations so far: {len(self.tried)}"):
            write_json(REPORT, {"day": today})


def single_instance():
    """Exit quietly if another miner is already running on this machine."""
    DIR.mkdir(exist_ok=True)
    fh = open(LOCK, "w")
    try:
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("miner is already running")
        sys.exit(0)
    return fh


def show_ready():
    m = Miner()
    m.refresh_references()
    print(f"{len(m.ready)} alphas ready to submit (best first):\n")
    for r in m.ready:
        checks = m.brain.check(r["alphaId"]).get("is", {}).get("checks", [])
        failed = [c["name"] for c in checks if c.get("result") == "FAIL"]
        s = r["settings"]
        print(f"{'OK  ' if not failed else 'NOW FAILS ' + ','.join(failed)} {r['alphaId']}  "
              f"Sharpe={r['sharpe']:.2f} Fitness={r['fitness']:.2f} Turnover={r['turnover']*100:.1f}%  "
              f"[{s['neutralization']}, decay {s['decay']}]\n      {r['expr']}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "ready":
        show_ready()
    else:
        _lock_handle = single_instance()
        Miner().run()
