"""Group progress: counts, denominator, ETA and stall detection. DESIGN_PHASE1 §21.7.

Pure functions over rows the store already holds, so every number on the page and in
the CLI comes from the same place and none of them is a stored counter.

The UX rules encoded here are not preferences; each one prevents a specific lie:

  * counts in user units ("12 of 40 agents"), because a bare percentage cannot be
    converted into a decision;
  * a denominator that never shrinks, because a bar that moves backward breaks the one
    promise the widget makes — so over-subscription revises it upward, visibly;
  * no ETA until enough members have finished to compute one honestly, because a wrong
    estimate is worse than none;
  * ETA from OBSERVED throughput rather than assumed parallelism, because real
    concurrency is capped by maxReplicaCount and the partition count — a 100-agent
    group runs about 6 at a time, not 100;
  * a stall stated in words, because freezing at 99% is the worst thing a progress
    display can do.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

# Below this many finished members the throughput sample is noise; show no estimate
# rather than a number that will visibly lurch.
MIN_FINISHED_FOR_ETA = 3
# Sliding window for the throughput measurement.
ETA_WINDOW_S = 60.0
# Silence longer than this is reported in words instead of as a frozen bar.
STALL_AFTER_S = 120.0


def _parse(ts: str | None) -> dt.datetime | None:
    if not ts:
        return None
    try:
        out = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return out if out.tzinfo else out.replace(tzinfo=dt.timezone.utc)
    except (ValueError, TypeError):
        return None


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def humanize(seconds: float | None) -> str | None:
    """Round aggressively. "about 4 minutes", never "3:47".

    Precision in an estimate is a claim you cannot support; Windows file copies became
    folklore by seesawing between 2 minutes and 4 hours.
    """
    if seconds is None or seconds < 0:
        return None
    if seconds < 45:
        return "less than a minute"
    minutes = seconds / 60.0
    if minutes < 20:
        return f"about {round(minutes)} minute{'s' if round(minutes) != 1 else ''}"
    if minutes < 90:
        # Past 20 minutes, a to-the-minute figure implies precision we do not have.
        return f"about {int(round(minutes / 5.0) * 5)} minutes"
    return f"about {round(minutes / 60.0, 1)} hours"


def elapsed_str(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"


def progress(group: dict[str, Any], members: list[dict[str, Any]],
             *, now: dt.datetime | None = None) -> dict[str, Any]:
    """Everything the page and the CLI need, derived from rows.

    `members` is the full member list; counts are taken from it rather than from
    `Store.group_counts` so a caller has exactly one source of truth per render.
    """
    now = now or _now()
    counts = {"submitted": 0, "running": 0, "finished": 0, "failed": 0}
    for m in members:
        counts[m["status"]] = counts.get(m["status"], 0) + 1
    seen = len(members)
    terminal = counts["finished"] + counts["failed"]

    # The denominator never shrinks. A caller that declares 40 and submits 43 gets a
    # visibly revised total, not a percentage that jumps backward.
    expected = group.get("expected")
    denominator = max(expected, seen) if expected else (seen or None)
    revised = bool(expected and seen > expected)

    pct = None
    if denominator:
        pct = round(100.0 * terminal / denominator, 1)

    started = min((_parse(m["submitted_utc"]) for m in members
                   if m.get("submitted_utc")), default=None) or _parse(group.get("created_utc"))
    completed = _parse(group.get("completed_utc"))
    end = completed or now
    elapsed = (end - started).total_seconds() if started else None

    # Throughput over a sliding window, measured rather than assumed.
    finishes = sorted(t for t in (_parse(m["finished_utc"]) for m in members) if t)
    eta_s = None
    throughput = None
    remaining = max(0, (denominator or 0) - terminal)
    if not completed and len(finishes) >= MIN_FINISHED_FOR_ETA and remaining > 0:
        window_start = now - dt.timedelta(seconds=ETA_WINDOW_S)
        in_window = [t for t in finishes if t >= window_start]
        if len(in_window) >= 2:
            span = (in_window[-1] - in_window[0]).total_seconds() or 1.0
            throughput = (len(in_window) - 1) / span
        elif elapsed and elapsed > 0:
            throughput = len(finishes) / elapsed
        if throughput and throughput > 0:
            eta_s = remaining / throughput

    # A stall is silence, and silence must be explained rather than animated.
    last_activity = max([t for t in (
        *(_parse(m["finished_utc"]) for m in members),
        *(_parse(m["first_event_utc"]) for m in members),
    ) if t] or [started] if started else [], default=None)
    stalled_for = None
    if not completed and last_activity:
        idle = (now - last_activity).total_seconds()
        if idle > STALL_AFTER_S:
            stalled_for = idle

    state = "running"
    if group.get("completed_utc"):
        state = group.get("completion_reason") or "complete"
    elif group.get("cancelled_utc"):
        state = "cancelled"

    return {
        "groupid": group.get("groupid"),
        "label": group.get("label"),
        "state": state,
        "expected": expected,
        "denominator": denominator,
        "denominator_revised": revised,
        "members_seen": seen,
        "queued": counts["submitted"],
        "running": counts["running"],
        "finished": counts["finished"],
        "failed": counts["failed"],
        "terminal": terminal,
        "remaining": remaining,
        "percent": pct,
        "elapsed_s": elapsed,
        "elapsed": elapsed_str(elapsed),
        "eta_s": eta_s,
        "eta": humanize(eta_s),
        "throughput_per_min": round(throughput * 60, 1) if throughput else None,
        "stalled_for_s": stalled_for,
        "stall_note": (f"no activity for {elapsed_str(stalled_for)}"
                       if stalled_for else None),
        "completed_utc": group.get("completed_utc"),
        "completion_reason": group.get("completion_reason"),
        "min_success": group.get("min_success"),
        "deadline_utc": group.get("deadline_utc"),
    }


def completion_reason(group: dict[str, Any], counts: dict[str, int],
                      *, now: dt.datetime | None = None) -> str | None:
    """Why this group should complete, or None if it should not yet.

    Order matters: cancellation and the deadline win over the count, so a cancelled or
    expired group cannot also claim it finished everything.
    """
    if group.get("completed_utc"):
        return None
    if group.get("cancelled_utc"):
        return "cancelled"
    deadline = _parse(group.get("deadline_utc"))
    if deadline and (now or _now()) >= deadline:
        return "deadline"

    terminal = counts.get("terminal", 0)
    min_success = group.get("min_success")
    if min_success and counts.get("finished", 0) >= min_success:
        return "quorum"

    expected = group.get("expected")
    if expected:
        # `>=` not `==`: an over-subscribed group must still be able to complete.
        return "all" if terminal >= expected else None
    # Open-ended: only a closed group can complete on count.
    if group.get("closed_utc") and terminal >= counts.get("members", 0) > 0:
        return "all"
    return None


def summary_line(p: dict[str, Any]) -> str:
    """One glanceable line, counts before percentage, for the CLI and notifications."""
    bits = [f"{p['terminal']}/{p['denominator'] or '?'} done"]
    if p["failed"]:
        bits.append(f"{p['failed']} failed")
    if p["running"]:
        bits.append(f"{p['running']} running")
    if p["queued"]:
        bits.append(f"{p['queued']} queued")
    bits.append(f"{p['elapsed']} elapsed")
    if p["eta"]:
        bits.append(f"{p['eta']} left")
    if p["stall_note"]:
        bits.append(p["stall_note"])
    return " · ".join(bits)
