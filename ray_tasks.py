import ray
import pickle
import os
import pandas as pd

TARGETS = ["kills", "assists", "adr", "kast", "kddiff"]
# METRICS includes deaths so its dataset average is available for inverse scoring
METRICS = ["kills", "assists", "adr", "kast", "kddiff", "deaths"]

# ── Precompute dataset averages at import time (tiny dict, not the full df) ──
# Only 5 floats stored — replaces loading the entire player DataFrame.
def _safe_float(value, default=0.0):
    """Convert a value to float, returning default for None, NaN, empty, or non-numeric input."""
    if value is None:
        return default
    try:
        result = float(value)
        # Reject NaN and Inf — both would corrupt averages
        if result != result or result == float("inf") or result == float("-inf"):
            return default
        return result
    except (ValueError, TypeError):
        return default

def _compute_averages(path="data/cs2_dataset.csv"):
    df_raw = pd.read_csv(path)
    totals            = {m: 0.0 for m in METRICS}
    kddiff_abs_total  = 0.0   # sum of |kddiff| for stable denominator
    kddiff_abs_count  = 0
    count             = 0

    for _, row in df_raw.iterrows():
        for team in ["team1", "team2"]:
            for i in range(1, 6):
                for m in METRICS:
                    raw = row.get(f"{team}_player{i}_{m}", 0)
                    val = _safe_float(raw)
                    totals[m] += val
                    if m == "kddiff":
                        kddiff_abs_total += abs(val)
                        kddiff_abs_count += 1
                count += 1

    averages = {m: round(totals[m] / count, 2) if count else 0.0 for m in METRICS}

    # kddiff average = mean of absolute values, floored at 3.0 to prevent a
    # near-zero denominator in datasets where most players have tiny KD Diffs.
    KDDIFF_SMOOTH_FLOOR = 3.0
    abs_avg = (kddiff_abs_total / kddiff_abs_count) if kddiff_abs_count > 0 else KDDIFF_SMOOTH_FLOOR
    averages["kddiff"] = round(max(abs_avg, KDDIFF_SMOOTH_FLOOR), 2)

    return averages

dataset_averages = _compute_averages()   # kddiff avg = mean of |kddiff|, floored at 3.0

# ── Lazy Ray object store references ─────────────────────────────────────────
# avgs_ref   → tiny dict (5 floats) put into object store once
# models_ref → not pre-loaded; each worker loads its own model from disk
# df_ref     → kept for app.py import compatibility, not used
avgs_ref   = None
models_ref = None
df_ref     = None

def init_ray_objects():
    """Called lazily on first request — puts only the lightweight averages dict."""
    global avgs_ref
    if avgs_ref is None:
        avgs_ref = ray.put(dataset_averages)   # negligible memory


# ── Interpretation helpers ────────────────────────────────────────────────────
def _interpret_prediction(value, target):
    thresholds = {
        "kills":   [(20, "Elite fragging expected"),    (14, "Strong performance expected"),
                    (8,  "Moderate output expected"),   (0,  "Low kill count expected")],
        "assists": [(10, "High playmaking expected"),   (6,  "Solid support expected"),
                    (3,  "Moderate assist count"),      (0,  "Low assist contribution")],
        "adr":     [(90, "Dominant damage output"),     (70, "Strong damage output"),
                    (50, "Average damage output"),      (0,  "Low damage output")],
        "kast":    [(80, "Highly consistent rounds"),   (65, "Good round consistency"),
                    (50, "Average consistency"),        (0,  "Inconsistent performance")],
        "kddiff":  [(5,  "Highly positive impact"),     (0,  "Slight positive impact"),
                    (-5, "Slight negative impact"),     (-999, "Significant negative impact")],
    }
    for threshold, message in thresholds.get(target, []):
        if value >= threshold:
            return message
    return "Prediction complete"


STAT_LABELS = {
    "kills":   "Kills",
    "assists": "Assists",
    "adr":     "ADR",
    "kast":    "KAST %",
    "kddiff":  "KD Diff",
    "deaths":  "Deaths",
}

IMPROVEMENT_TIPS = {
    "kills":   "Focus on positioning and aim training to increase kill count.",
    "assists": "Play more utility and support roles; trade-fragging boosts assists.",
    "adr":     "Engage more aggressively early in rounds to raise damage output.",
    "kast":    "Survive rounds longer and prioritize safer engagements.",
    "kddiff":  "Trade deaths for kills more efficiently; avoid unnecessary duels.",
    "deaths":  "Improve survival instincts — avoid overpeeks and unnecessary duels to reduce deaths.",
}


# ── Ray Task: ML Prediction ───────────────────────────────────────────────────
# Model is loaded from disk inside the worker — no large object store entry.
@ray.remote
def predict_player(data, target):
    try:
        model_path = f"model/model_{target}.pkl"
        if not os.path.exists(model_path):
            return {"error": f"No trained model found for target '{target}'"}

        with open(model_path, "rb") as f:
            model, features = pickle.load(f)

        vector          = {ft: float(data.get(ft, 0)) for ft in features}
        input_df        = pd.DataFrame([vector], columns=features)
        raw_value       = model.predict(input_df)[0]
        predicted_value = round(raw_value, 2)
        message         = _interpret_prediction(predicted_value, target)

        return {
            "player":          data.get("name", "Unknown"),
            "target":          target,
            "target_label":    STAT_LABELS.get(target, target.upper()),
            "predicted_value": predicted_value,
            "message":         message,
        }

    except Exception as e:
        return {"error": str(e)}


# ── Ray Task: Statistical Analysis ───────────────────────────────────────────
# Receives a tiny averages dict (5 floats) — not the full dataset.
@ray.remote
def analyze_player(data, averages):
    try:
        # Read player values — sanitize missing/NaN/invalid entries to 0.0
        # Include deaths alongside the METRICS set
        ALL_ANALYZE = ["kills", "assists", "adr", "kast", "kddiff", "deaths"]
        player_vals = {}
        for m in ALL_ANALYZE:
            player_vals[m] = _safe_float(data.get(m, 0))

        # ── Per-metric scores ─────────────────────────────────────────────────
        # Two scoring strategies:
        #   1. Ratio-based (higher = better): kills, assists, adr, kast, kddiff
        #      kddiff uses avg-of-absolute-values as denominator for stability,
        #      then reapplies the original sign so direction is preserved.
        #   2. Inverse ratio (lower = better): deaths

        RATIO_METRICS = ["kills", "assists", "adr", "kast"]
        ratios = {}

        for m in RATIO_METRICS:
            avg = averages.get(m, 1.0)
            ratios[m] = (player_vals[m] / avg) if avg != 0 else 1.0

        # kddiff ratio-based scoring — sign-preserved magnitude normalization:
        #   1. Normalize magnitude:  abs(player_kddiff) / abs_avg  → always ≥ 0
        #   2. Reapply original sign so positive kddiff → score > 0, negative → score < 0
        #   3. Shift by +1.0 so a player at exactly +abs_avg maps to 1.0 (average),
        #      zero kddiff maps to 1.0 (neutral), and negative kddiff maps below 1.0
        #   4. Clamp to [0.0, 2.0] so extreme outliers cannot dominate the composite
        kddiff_abs_avg   = averages.get("kddiff", 3.0)   # always ≥ 3.0 after _compute_averages
        kddiff_val       = player_vals["kddiff"]
        kddiff_sign      = 1 if kddiff_val >= 0 else -1
        kddiff_magnitude = abs(kddiff_val) / kddiff_abs_avg          # normalised magnitude
        kddiff_signed    = kddiff_sign * kddiff_magnitude             # sign reapplied
        kddiff_ratio     = round(min(max(1.0 + kddiff_signed, 0.0), 2.0), 3)
        ratios["kddiff"] = kddiff_ratio

        # kddiff arrow: positive val → above avg direction, negative → below
        kddiff_delta = kddiff_val   # sign alone determines arrow; magnitude shown in detail

        # deaths: inverse ratio — avg_deaths / player_deaths
        # fewer deaths than average → score > 1.0 (good); more deaths → score < 1.0 (bad)
        deaths_avg = averages.get("deaths", 1.0)
        if player_vals["deaths"] > 0:
            ratios["deaths"] = deaths_avg / player_vals["deaths"]
        else:
            ratios["deaths"] = 2.0   # zero deaths → maximum reward, cap at 2×

        # Composite score across all six metrics
        composite_score = round(sum(ratios.values()) / len(ratios), 3)
        performance     = "Above Average" if composite_score >= 1.0 else "Below Average"

        # strong/weak: deaths uses inverted logic (low deaths → high score → strong)
        strong_stats = [m for m, r in ratios.items() if r >= 1.1]
        weak_stats   = [m for m, r in ratios.items() if r < 0.9]

        # Detail string — arrows reflect actual performance direction
        detail_parts = []
        ALL_DISPLAY = ["kills", "assists", "adr", "kast", "kddiff", "deaths"]
        for m in ALL_DISPLAY:
            if m == "kddiff":
                arrow = "▲" if kddiff_delta > 0 else ("▼" if kddiff_delta < 0 else "●")
            elif m == "deaths":
                # arrow is ▲ (good) when player deaths < avg deaths
                arrow = "▲" if player_vals["deaths"] < deaths_avg else ("▼" if player_vals["deaths"] > deaths_avg else "●")
            else:
                arrow = "▲" if ratios[m] >= 1.1 else ("▼" if ratios[m] < 0.9 else "●")
            detail_parts.append(
                f"{arrow} {STAT_LABELS[m]}: {player_vals[m]} (avg {averages.get(m, 0)})"
            )
        details = " | ".join(detail_parts)

        # Overall message
        if composite_score >= 1.2:
            message = "Outstanding overall performance — well above the dataset average."
        elif composite_score >= 1.0:
            message = "Solid performance — you are performing above the average player."
        elif composite_score >= 0.8:
            message = "Performance is close to average — small improvements can make a big difference."
        else:
            message = "Performance is below average — significant room for improvement."

        improvements = [IMPROVEMENT_TIPS[m] for m in weak_stats if m in IMPROVEMENT_TIPS]

        graph_labels        = [STAT_LABELS[m] for m in ALL_DISPLAY]
        graph_player_values = [player_vals[m] for m in ALL_DISPLAY]
        graph_avg_values    = [averages.get(m, 0) for m in ALL_DISPLAY]

        return {
            "player":       data.get("name", "Unknown"),
            "score":        composite_score,
            "performance":  performance,
            "message":      message,
            "details":      details,
            "strong_stats": [STAT_LABELS[m] for m in strong_stats],
            "weak_stats":   [STAT_LABELS[m] for m in weak_stats],
            "improvements": improvements,
            "graph": {
                "labels":        graph_labels,
                "player_values": graph_player_values,
                "avg_values":    graph_avg_values,
            },
        }

    except Exception as e:
        return {"error": str(e)}
