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
import statistics
import sys
import threading
import time
import traceback
from collections import defaultdict, deque
from datetime import date, datetime
from pathlib import Path

import requests

from brain import Brain, CONCURRENCY, DEFAULT_SETTINGS, summarize

# ---- knobs (environment variables override the defaults) ------------------------------------
MINUTES = int(os.environ.get("MINER_MINUTES", 0))                     # 0 = run forever
MAX_SIMS_PER_DAY = int(os.environ.get("MINER_MAX_SIMS_PER_DAY", 600))  # BRAIN asks for low-intensity API use
MIN_FITNESS = 1.0          # submission cutoff for Delay 1
PROMISING_SHARPE = 1.0     # stage-1 |Sharpe| that earns a tuning family
MIN_STABILITY = 0.34       # share of a family's variants that must reach Sharpe 1.0 (guards against lucky parameters)
MAX_BAD_YEARS = 1          # at most this many losing years in the in-sample period
MAX_CORRELATION = 0.7      # same cutoff BRAIN uses for self-correlation
MIN_FIELD_COVERAGE = 0.3
FIELDS_MAX_AGE_DAYS = 7
SCOPE = {"region": "USA", "delay": 1, "universe": "TOP3000"}

DIR = Path(__file__).parent / "miner"
TRIED = DIR / "tried.jsonl"        # one line per simulation ever run (resume + dedupe)
FAMILIES = DIR / "families.json"   # tuning families still in progress
READY = DIR / "ready.json"         # alphas worth submitting, best first
FIELDS = DIR / "fields.json"       # cached field catalogue
STATE = DIR / "state.json"         # daily simulation counter
NEW_READY = DIR / "new_ready.md"   # winners found in this run (used for notifications)
LOG = DIR / "miner.log"
LOCK = DIR / "miner.lock"

# ---- idea templates -----------------------------------------------------------------------
# {x} = the field (backfilled when sparse), {w} = lookback window in trading days.
# Pass 1 runs over every field first; pass 2 templates follow once pass 1 is exhausted.
TEMPLATES = {
    "tsrank_ind": {"expr": "group_rank(ts_rank({x}, {w}), industry)", "windows": [60, 120, 252],
                   "neutralization": "INDUSTRY", "pass": 1},
    "zscore":     {"expr": "ts_zscore({x}, {w})", "windows": [60, 126, 252],
                   "neutralization": "SUBINDUSTRY", "pass": 1},
    "level_sub":  {"expr": "group_rank({x}, subindustry)", "windows": [None],
                   "neutralization": "SUBINDUSTRY", "pass": 2},
    "delta":      {"expr": "group_rank(ts_delta({x}, {w}), subindustry)", "windows": [22, 66, 252],
                   "neutralization": "SUBINDUSTRY", "pass": 2},
    "to_cap":     {"expr": "group_rank({x}/cap, subindustry)", "windows": [None],
                   "neutralization": "SUBINDUSTRY", "pass": 2, "categories": {"fundamental", "analyst"}},
}
BASE = {"nanHandling": "ON", "truncation": 0.08}
STAGE1_DECAY = 4
TUNE_DECAYS = [3, 8]
TUNE_NEUTRALIZATIONS = ["INDUSTRY", "SUBINDUSTRY"]

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

    # ---- inputs ---------------------------------------------------------------------------
    def load_fields(self, force=False):
        if not force and FIELDS.exists() and time.time() - FIELDS.stat().st_mtime < FIELDS_MAX_AGE_DAYS * 86400:
            return json.loads(FIELDS.read_text(encoding="utf-8"))
        per_dataset = {}
        for ds in self.brain.datasets(**SCOPE):
            try:
                fs = [f for f in self.brain.data_fields(ds["id"], **SCOPE)
                      if f["type"] in ("MATRIX", "VECTOR") and (f.get("coverage") or 0) >= MIN_FIELD_COVERAGE]
            except requests.RequestException as e:
                log(f"fields: {ds['id']} failed: {e}", public=True)
                continue
            # Least-crowded fields first: fewer existing alphas means a better chance of low correlation.
            per_dataset[ds["id"]] = sorted(fs, key=lambda f: f.get("alphaCount") or 0)
            log(f"fields: {ds['id']:16} {len(fs):5} usable  ({ds['name']})", public=True)
        # Round-robin across datasets so every dataset gets explored early.
        ordered = [f for group in itertools.zip_longest(*per_dataset.values()) for f in group if f]
        slim = [{"id": f["id"], "type": f["type"], "coverage": f.get("coverage") or 0,
                 "dataset": f["dataset"]["id"], "category": (f.get("category") or {}).get("id", "")}
                for f in ordered]
        write_json(FIELDS, slim)
        return slim

    def _stage1_jobs(self):
        """Endless stream of untried stage-1 jobs: pass 1 over all fields, then pass 2, then refresh."""
        while True:
            new = 0
            fields = self.load_fields()
            for p in (1, 2):
                for f in fields:
                    x = f["id"] if f["type"] == "MATRIX" else f"vec_avg({f['id']})"
                    if f["coverage"] < 0.8:
                        x = f"ts_backfill({x}, 66)"
                    for name, t in TEMPLATES.items():
                        if t["pass"] != p or ("categories" in t and f["category"] not in t["categories"]):
                            continue
                        job = {"stage": 1, "template": name, "x": x, "dataset": f["dataset"],
                               "expr": t["expr"].format(x=x, w=t["windows"][len(t["windows"]) // 2]),
                               "settings": {**BASE, "neutralization": t["neutralization"], "decay": STAGE1_DECAY}}
                        if key(job["expr"], job["settings"]) not in self.tried:
                            new += 1
                            yield job
            if not new:
                log("every field and template has been tried; checking for new data fields every 6 hours", public=True)
                until = time.time() + 6 * 3600
                while time.time() < until:
                    yield None  # tells the worker to idle (never sleep here: the caller holds the lock)
                self.load_fields(force=True)

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
        for w, d, n in itertools.product(t["windows"], TUNE_DECAYS, TUNE_NEUTRALIZATIONS):
            expr = t["expr"].format(x=job["x"], w=w)
            jobs.append({"expr": f"-{expr}" if sign < 0 else expr,
                         "settings": {**BASE, "neutralization": n, "decay": d}})
        fid = hashlib.sha1(f"{job['template']}|{job['x']}|{sign}".encode()).hexdigest()[:12]
        with _lock:
            if fid in self.families:
                return
            self.families[fid] = {"template": job["template"], "x": job["x"], "dataset": job.get("dataset"),
                                  "sign": sign, "jobs": jobs}
            write_json(FAMILIES, self.families)
        log(f"  promising (S={r['sharpe']:.2f}) -> tuning {len(jobs)} variants: {job['expr']}")
        self.schedule_family(fid)

    def schedule_family(self, fid):
        fam = self.families[fid]
        pending = [j for j in fam["jobs"] if key(j["expr"], j["settings"]) not in self.tried]
        with _lock:
            for j in pending:
                self.priority.append({"stage": 2, "template": fam["template"], "x": fam["x"],
                                      "dataset": fam.get("dataset"), "fid": fid, **j})
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
        stability = sum(r["sharpe"] >= PROMISING_SHARPE for r in ok) / len(ok)
        passing = sorted((r for r in ok if not fails(r) and (r["fitness"] or 0) >= MIN_FITNESS and r["sharpe"] > 0),
                         key=lambda r: -r["fitness"])
        best = max(ok, key=lambda r: r["fitness"] or -9)
        log(f"  family done: {fam['template']} {fam['x']} -> {len(passing)}/{len(ok)} pass, "
            f"stability {stability:.0%}, best F={best['fitness']}")
        if passing and stability < MIN_STABILITY:
            log("  rejected: only a few parameter choices work (likely luck, would not hold up out-of-sample)")
            return
        for r in passing[:3]:
            if self.consider(r, stability):
                return

    def consider(self, r, stability):
        """Returns True once the idea is settled (accepted or already covered), False to try the next variant."""
        pnl = self.brain.daily_pnl(r["alphaId"])
        years = yearly_sharpe(pnl)
        bad_years = [y for y, s in years.items() if s <= 0]
        if len(years) < 3 or len(bad_years) > MAX_BAD_YEARS:
            log(f"  {r['alphaId']} rejected: losing years {bad_years} {years}")
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
        return True

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
        last_refresh = time.time()
        while any(t.is_alive() for t in threads):
            time.sleep(15)
            if deadline and time.time() >= deadline and not self.stop.is_set():
                log("time is up; finishing the simulations in flight", public=True)
                self.stop.set()
            if time.time() - last_refresh > 3600:
                last_refresh = time.time()
                try:
                    self.refresh_references()
                except requests.RequestException as e:
                    log(f"refresh failed: {e}", public=True)
        log(f"run finished: {self.run_sims} simulations, {self.run_winners} new alphas ready, "
            f"{len(self.ready)} on the ready list, {len(self.families)} families still tuning", public=True)


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
