"""Lab S1 — полный прогон: проверка ALOHA, сетка N×SF×период, max N при PDR ≥ 95 %,
энергобюджет и battery lifetime, сценарий ADR. Результаты -> ../results (или $RESULTS).

Запуск:  python run_all.py            (~1–3 мин)
         python run_all.py --quick    (1 seed, быстрая проверка)
"""
import argparse
import csv
import json
import math
import os
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

import energy  # noqa: E402
import network as nw  # noqa: E402
from airtime import time_on_air  # noqa: E402

OUT = os.getenv("RESULTS", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results"))
SFS = [7, 9, 12]
PERIODS = [300, 900, 3600]
N_GRID = [100, 250, 500, 1000, 2000, 5000]       # 100/500/1000/5000 из задания + 2 точки для формы кривой
PAYLOAD = 20
T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776}

# палитра проверена валидатором (CVD, контраст) + маркеры как вторичная кодировка
SF_COLOR = {7: "#2a78d6", 9: "#eb6834", 12: "#1baf7a"}
SF_MARK = {7: "o", 9: "s", 12: "^"}
EXTRA = ["#eda100", "#e87ba4", "#6250d6"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({
    "figure.dpi": 150, "font.size": 10, "axes.edgecolor": INK2, "axes.labelcolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.spines.top": False, "axes.spines.right": False, "axes.axisbelow": True,
    "legend.frameon": False, "lines.linewidth": 2, "figure.facecolor": "white",
})


def ci(v):
    v = np.asarray(v, dtype=float)
    if len(v) < 2:
        return float(v.mean()), 0.0
    return float(v.mean()), T975.get(len(v) - 1, 1.96) * float(v.std(ddof=1)) / math.sqrt(len(v))


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def save(fig, name, title):
    fig.axes[0].set_title(title, loc="left", fontsize=11, color=INK, pad=10) if title else None
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, name), facecolor="white")
    plt.close(fig)


def write_csv(name, rows):
    with open(os.path.join(OUT, name), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    a = ap.parse_args()
    seeds = [1] if a.quick else [1, 2, 3]
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    summary = {"seeds": len(seeds), "payload_bytes": PAYLOAD, "channels": nw.CHANNELS,
               "toa_ms": {sf: round(time_on_air(sf, PAYLOAD) * 1000, 1) for sf in range(7, 13)}}

    # ------------------------------------------------------------ 1. проверка на пределе ALOHA
    log("1/5 ALOHA validation")
    gs = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.75, 1.0, 1.25, 1.5, 2.0]
    al = []
    for g in gs:
        m, h = ci([nw.aloha_test(g, seed=s) for s in seeds])
        al.append({"G": g, "S_sim": round(m, 4), "S_ci": round(h, 4), "S_theory": round(g * math.exp(-2 * g), 4)})
    write_csv("aloha_validation.csv", al)
    best = max(al, key=lambda r: r["S_sim"])
    summary["aloha"] = {"S_max_sim": best["S_sim"], "G_at_max": best["G"], "S_max_theory": round(1 / (2 * math.e), 4)}
    log(f"   S_max sim = {best['S_sim']} at G = {best['G']} (theory 0.1839 at G = 0.5)")

    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    gg = np.linspace(0, 2.05, 300)
    ax.plot(gg, gg * np.exp(-2 * gg), color=INK2, linewidth=1.4, label="теория: S = G·e^(−2G)")
    ax.errorbar([r["G"] for r in al], [r["S_sim"] for r in al], yerr=[r["S_ci"] for r in al], fmt="o",
                color=SF_COLOR[7], markersize=7, markeredgecolor="white", markeredgewidth=1.5, capsize=3,
                label="симуляция (среднее ± 95 % ДИ)")
    ax.axhline(1 / (2 * math.e), color=INK2, linestyle=":", linewidth=0.9)
    ax.annotate(f"максимум 18,4 % при G = 0,5\nсимуляция: {best['S_sim'] * 100:.1f} % при G = {best['G']}".replace(".", ","),
                (0.5, 0.184), xytext=(1.0, 0.245), fontsize=9, color=INK, va="top",
                arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))
    ax.set_xlabel("Нагрузка G, передач за время пакета")
    ax.set_ylabel("Пропускная способность S")
    ax.set_ylim(0, 0.26)
    ax.legend(loc="upper left", fontsize=8)
    save(fig, "fig1_aloha_validation.png", "Проверка модели на пределе чистой ALOHA")

    # ------------------------------------------------------------ 2. сетка N × SF × период
    log("2/5 PDR grid")
    rows = []
    for period in PERIODS:
        for sf in SFS:
            for n in N_GRID:
                for cap in ((True, False) if period == 900 else (True,)):
                    runs = [nw.simulate(n, sf, period, PAYLOAD, capture=cap, seed=s) for s in seeds]
                    m, h = ci([r["pdr"] for r in runs])
                    rows.append({"period_s": period, "sf": sf, "n": n, "capture": cap,
                                 "pdr": round(m, 4), "pdr_ci": round(h, 4),
                                 "pdr_analytic_no_capture": round(nw.analytic_pdr(n, sf, period, PAYLOAD), 4),
                                 "G_per_channel": round(n * time_on_air(sf, PAYLOAD) / (period * nw.CHANNELS), 4),
                                 "packets": sum(r["sent"] for r in runs)})
        log(f"   period {period} s done")
    write_csv("pdr_grid.csv", rows)

    def series(period, sf, cap=True):
        r = [x for x in rows if x["period_s"] == period and x["sf"] == sf and x["capture"] == cap]
        return [x["n"] for x in r], [x["pdr"] * 100 for x in r], [x["pdr_ci"] * 100 for x in r]

    fig, ax = plt.subplots(figsize=(6.6, 3.9))
    for sf in SFS:
        n, p, h = series(900, sf, True)
        ax.errorbar(n, p, yerr=h, color=SF_COLOR[sf], marker=SF_MARK[sf], markersize=7, capsize=3,
                    markeredgecolor="white", markeredgewidth=1.5, label=f"SF{sf}, с capture")
        n, p, _ = series(900, sf, False)
        ax.plot(n, p, "--", color=SF_COLOR[sf], linewidth=1.3, label=f"SF{sf}, без capture")
    ax.axhline(95, color=INK2, linestyle=":", linewidth=1)
    ax.text(5200, 96.5, "PDR = 95 %", fontsize=9, color=INK2, ha="right")
    ax.set_xscale("log")
    ax.set_xlabel("Число устройств N (логарифмическая шкала)")
    ax.set_ylabel("PDR, %")
    ax.set_ylim(0, 102)
    ax.legend(loc="lower left", fontsize=8, ncol=2)
    save(fig, "fig2_pdr_n_sf.png", "PDR(N) для SF7/SF9/SF12, период 15 мин")

    fig, axs = plt.subplots(1, 3, figsize=(9.6, 3.4), sharey=True)
    for ax, period in zip(axs, PERIODS):
        for sf in SFS:
            n, p, h = series(period, sf)
            ax.errorbar(n, p, yerr=h, color=SF_COLOR[sf], marker=SF_MARK[sf], markersize=6, capsize=2,
                        markeredgecolor="white", markeredgewidth=1.2, label=f"SF{sf}")
        ax.axhline(95, color=INK2, linestyle=":", linewidth=1)
        ax.set_xscale("log")
        ax.set_title(f"период {period // 60} мин", fontsize=10, color=INK, loc="left")
        ax.set_xlabel("N")
    axs[0].set_ylabel("PDR, %")
    axs[0].set_ylim(0, 102)
    axs[0].legend(loc="lower left", fontsize=8)
    fig.suptitle("PDR(N) при разной частоте передачи (с capture effect)", x=0.01, ha="left", fontsize=11, color=INK)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "fig3_pdr_periods.png"), facecolor="white")
    plt.close(fig)

    # ------------------------------------------------------------ 3. max N при PDR >= 95 %
    log("3/5 max N for PDR >= 95 %")
    maxn = []
    for period in PERIODS:
        for sf in SFS:
            def pdr_of(n):
                return np.mean([nw.simulate(int(n), sf, period, PAYLOAD, seed=s)["pdr"] for s in seeds])
            lo, hi = 1, max(20, int(nw.analytic_max_n(sf, period) * 4))
            while hi - lo > max(1, lo // 50):           # бисекция с точностью ~2 %
                mid = (lo + hi) // 2
                lo, hi = (mid, hi) if pdr_of(mid) >= 0.95 else (lo, mid)
            maxn.append({"period_s": period, "sf": sf, "toa_ms": round(time_on_air(sf, PAYLOAD) * 1000, 1),
                         "max_n_sim_capture": lo,
                         "max_n_analytic_no_capture": int(nw.analytic_max_n(sf, period)),
                         "per_device_duty_cycle_pct": round(100 * time_on_air(sf, PAYLOAD) / period, 3)})
            log(f"   T={period}s SF{sf}: N_max = {lo} (analytic w/o capture {maxn[-1]['max_n_analytic_no_capture']})")
    write_csv("max_devices.csv", maxn)
    summary["max_devices"] = maxn

    # ------------------------------------------------------------ 4. энергия и battery lifetime
    log("4/5 energy model")
    en = []
    for sf in SFS:
        for period in PERIODS:
            b = energy.budget(sf, period, PAYLOAD)
            en.append({"sf": sf, "period_s": period, "toa_ms": round(b["toa_ms"], 1),
                       "e_msg_mJ": round(b["e_msg_mJ"], 1), "i_avg_uA": round(b["i_avg_uA"], 1),
                       "lifetime_years_model": round(b["years_raw"], 1), "lifetime_years_capped": round(b["years"], 1),
                       "share_tx_pct": round(100 * b["share"]["tx"], 1),
                       "share_rx_pct": round(100 * (b["share"]["rx1"] + b["share"]["rx2"]), 1),
                       "share_rx2_pct": round(100 * b["share"]["rx2"], 1),
                       "share_sleep_pct": round(100 * b["share"]["sleep"], 1),
                       "share_other_pct": round(100 * (b["share"]["wake"] + b["share"]["wait"]), 1)})
    write_csv("battery_lifetime.csv", en)

    fig, ax = plt.subplots(figsize=(6.6, 3.2))
    phases = [("tx", "TX (передача)", SF_COLOR[7]), ("rx1", "RX1 (тот же SF)", SF_COLOR[9]),
              ("rx2", "RX2 (SF12, 869,525 МГц)", SF_COLOR[12]), ("wake", "пробуждение + датчик", EXTRA[0])]
    y = np.arange(len(SFS))[::-1]
    left = np.zeros(len(SFS))
    totals = []
    for sf in SFS:
        q = energy.cycle(sf, PAYLOAD)[0]
        totals.append(sum(q[k] for k, _, _ in phases))
    for key, label, col in phases:
        share = np.array([energy.cycle(sf, PAYLOAD)[0][key] / t * 100 for sf, t in zip(SFS, totals)])
        ax.barh(y, share, 0.55, left=left, color=col, edgecolor="white", linewidth=2, label=label)
        for yi, l0, sh in zip(y, left, share):
            if sh >= 7:
                ax.text(l0 + sh / 2, yi, f"{sh:.0f} %", ha="center", va="center", fontsize=9, color="white")
        left += share
    ax.set_yticks(y, [f"SF{sf}\n{t * energy.V * 1000:.0f} мДж" for sf, t in zip(SFS, totals)])
    ax.set_xlim(0, 100)
    ax.set_xlabel("Доля энергии одного цикла передачи, % (без сна)")
    ax.grid(axis="y", visible=False)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=2, fontsize=8)
    save(fig, "fig4_energy_per_message.png", "Энергия цикла передачи по фазам")

    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    pp = np.geomspace(60, 86400, 200)
    for sf in SFS:
        y = [energy.budget(sf, p, PAYLOAD)["years_raw"] for p in pp]
        ax.plot(pp / 60, y, color=SF_COLOR[sf], label=f"SF{sf}")
        for period in PERIODS:
            yy = energy.budget(sf, period, PAYLOAD)["years_raw"]
            ax.plot(period / 60, yy, SF_MARK[sf], color=SF_COLOR[sf], markersize=7, markeredgecolor="white")
    ax.axhline(energy.SHELF_LIFE_Y, color=INK2, linestyle=":", linewidth=1)
    ax.text(1300, energy.SHELF_LIFE_Y * 0.85, "≈10 лет — предел\nсаморазряда батареи", fontsize=9, color=INK2,
            ha="right", va="top")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Период передачи, мин (лог.)")
    ax.set_ylabel("Срок службы, лет (лог.)")
    ax.legend(loc="upper left")
    save(fig, "fig5_battery_lifetime.png", "Срок службы батареи 2400 мА·ч в зависимости от периода")

    # ------------------------------------------------------------ 5. ADR: много устройств на SF12
    log("5/5 ADR scenario")
    n_adr, t_adr = 1000, 900
    dists = {
        "ADR по расстоянию": {7: .45, 8: .15, 9: .12, 10: .10, 11: .08, 12: .10},
        "ADR сдвинул 50 % на SF12": {7: .25, 8: .08, 9: .07, 10: .05, 11: .05, 12: .50},
        "все на SF12": {12: 1.0},
    }
    adr = []
    for name, d in dists.items():
        res = []
        for s in seeds:
            rng = np.random.default_rng(100 + s)
            sfs = rng.choice(list(d.keys()), size=n_adr, p=list(d.values()))
            res.append(nw.simulate(n_adr, 7, t_adr, PAYLOAD, seed=s, sf_per_device=sfs))
        row = {"scenario": name, "n": n_adr, "period_s": t_adr, "pdr": round(float(np.mean([r["pdr"] for r in res])), 4)}
        for sf in range(7, 13):
            v = [r.get(f"pdr_sf{sf}", np.nan) for r in res]
            row[f"pdr_sf{sf}"] = round(float(np.nanmean(v)), 4) if not np.all(np.isnan(v)) else ""
        e_avg = sum(p * energy.budget(sf, t_adr, PAYLOAD)["i_avg_uA"] for sf, p in d.items())
        row["avg_current_uA"] = round(e_avg, 1)
        row["airtime_share_pct"] = round(100 * sum(p * time_on_air(sf, PAYLOAD) for sf, p in d.items()) * n_adr / t_adr / nw.CHANNELS, 1)
        adr.append(row)
        log(f"   {name}: PDR {row['pdr'] * 100:.1f} %")
    write_csv("adr_scenario.csv", adr)

    fig, ax = plt.subplots(figsize=(6.6, 4.4))
    w = 0.26
    cols = [SF_COLOR[7], SF_COLOR[9], SF_COLOR[12]]
    xs = np.arange(7, 13)
    for i, r in enumerate(adr):
        vals = [r[f"pdr_sf{sf}"] * 100 if r[f"pdr_sf{sf}"] != "" else np.nan for sf in xs]
        ax.bar(np.arange(6) + (i - 1) * w, vals, w, color=cols[i], edgecolor="white", linewidth=1.5,
               label=f"{r['scenario']} (итого {r['pdr'] * 100:.0f} %)")
    ax.axhline(95, color=INK2, linestyle=":", linewidth=1)
    ax.set_xticks(np.arange(6), [f"SF{s}" for s in xs])
    ax.set_ylabel("PDR, %")
    ax.set_ylim(0, 105)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), fontsize=8, ncol=1)
    save(fig, "fig6_adr_sf12.png", f"Влияние распределения SF (N = {n_adr}, период 15 мин)")

    summary["elapsed_s"] = round(time.time() - t0, 1)
    json.dump(summary, open(os.path.join(OUT, "summary.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    log(f"Done in {summary['elapsed_s']} s -> {os.path.normpath(OUT)}")


if __name__ == "__main__":
    main()
