from flask import Flask, render_template, request
import ray

# ── Single Ray init — must happen before ray_tasks is imported ───────────────
ray.init(address="auto", ignore_reinit_error=True)

from ray_tasks import predict_player, analyze_player, init_ray_objects
import ray_tasks

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze():
    players = []

    names   = request.form.getlist("name")
    deaths  = request.form.getlist("deaths")
    assists = request.form.getlist("assists")
    adr     = request.form.getlist("adr")
    kast    = request.form.getlist("kast")
    kddiff  = request.form.getlist("kddiff")
    kills   = request.form.getlist("kills")   # only used in analyze mode

    mode   = request.form.get("mode", "predict")
    target = request.form.get("target", "kills")  # only used in predict mode

    for i in range(len(names)):
        players.append({
            "name":    names[i],
            "deaths":  deaths[i],
            "assists": assists[i],
            "adr":     adr[i],
            "kast":    kast[i],
            "kddiff":  kddiff[i],
            "kills":   kills[i] if i < len(kills) else "0",
            "target":  target,
        })

    # ── Lazy init: puts only the tiny averages dict into Ray object store ───
    if ray_tasks.avgs_ref is None:
        init_ray_objects()

    if mode == "predict":
        # Workers load their model from disk — no large object store entry
        futures = [predict_player.remote(p, p["target"]) for p in players]
    else:
        # Pass only the tiny averages ref — 5 floats, not the full dataset
        futures = [analyze_player.remote(p, ray_tasks.avgs_ref) for p in players]

    results = ray.get(futures)

    return render_template("result.html", results=results, mode=mode)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
