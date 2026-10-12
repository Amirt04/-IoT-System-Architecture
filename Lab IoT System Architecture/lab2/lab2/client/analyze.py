"""Lab 2 — обработка результатов: таблицы, доверительные интервалы, графики.

Вход:  /results/raw/*.json (от measure.py)
Выход: /results/summary.csv, percentiles.csv, connect.csv, nfr.txt, fig1..fig5 *.png
Доверительный интервал 95 %: mean ± t(0.975, n-1) * s / sqrt(n) по сериям.
"""
import csv
import glob
import json
import math
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from profiles import PROFILES, rssi_at  # noqa: E402

OUT = os.getenv("RESULTS", "/results")
T975 = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}

# NFR (Practice 2). Значения по умолчанию — поменяй в /results/nfr.json под свои
NFR_DEFAULT = {"mqtt_delivery_min_pct": 99.9,     # доля доставки при QoS 1
               "mqtt_p99_max_ms": 500,            # p99 задержки доставки
               "connect_p95_max_ms": 2000,        # p95 полного подключения TLS+MQTT (без Wi-Fi assoc/DHCP)
               "throughput_min_mbit": 1.0}        # минимальная полоса

# палитра (проверена валидатором: CVD/контраст) + разные маркеры как вторичная кодировка
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
MARKERS = ["o", "s", "^", "D", "v"]
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"

plt.rcParams.update({
    "figure.dpi": 150, "font.size": 10, "axes.edgecolor": INK2, "axes.labelcolor": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.spines.top": False, "axes.spines.right": False,
    "legend.frameon": False, "axes.axisbelow": True, "lines.linewidth": 2, "figure.facecolor": "white",
})


def ci(values):
    v = [x for x in values if x is not None]
    if not v:
        return None, None, 0
    m = float(np.mean(v))
    if len(v) < 2:
        return m, 0.0, 1
    h = T975.get(len(v) - 1, 1.96) * float(np.std(v, ddof=1)) / math.sqrt(len(v))
    return m, h, len(v)


def pct(values, q):
    return float(np.percentile(values, q)) if values else None


def load():
    data = defaultdict(list)
    for f in sorted(glob.glob(os.path.join(OUT, "raw", "*.json"))):
        r = json.load(open(f))
        data[r["profile"]["name"]].append(r)
    return data


def fmt(m, h, d=2):
    if m is None:
        return "–"
    return f"{m:.{d}f} ± {h:.{d}f}" if h else f"{m:.{d}f}"


def main():
    data = load()
    if not data:
        raise SystemExit("нет данных в results/raw — сначала запусти measure.py")
    nfr = dict(NFR_DEFAULT)
    nfr_path = os.path.join(OUT, "nfr.json")
    if os.path.exists(nfr_path):
        nfr.update(json.load(open(nfr_path)))
    else:
        json.dump(nfr, open(nfr_path, "w"), indent=2)

    profs = [p for p in PROFILES if p["name"] in data]
    rows, prow, crow, agg = [], [], [], {}
    for p in profs:
        runs = data[p["name"]]
        n = len(runs)
        thr = ci([r["iperf"]["throughput_mbit"] for r in runs])
        retr = ci([r["iperf"]["retransmits"] for r in runs])
        loss = ci([r["ping"]["loss_pct"] for r in runs])
        jit = ci([r["ping"]["jitter_ms"] for r in runs])
        rtt_all = [x for r in runs for x in r["ping"]["rtt_ms"]]
        p99_series = ci([pct(r["ping"]["rtt_ms"], 99) for r in runs if r["ping"]["rtt_ms"]])
        a = {"p": p, "thr": thr, "loss": loss, "jit": jit, "rtt": rtt_all, "n": n}
        for q in (0, 1):
            ms = [r["mqtt"][q] for r in runs]
            a[f"q{q}_loss"] = ci([m["loss_pct"] for m in ms])
            a[f"q{q}_lat"] = [x for m in ms for x in m["latency_ms"]]
            a[f"q{q}_dup"] = sum(m["duplicates"] for m in ms)
            a[f"q{q}_ack"] = [x for m in ms for x in m["puback_ms"]]
        conn = {k: [c for r in runs for c in r["connect"][k]] for k in ("plain", "tls")}
        fails = {k: sum(r["connect"]["failures"][k] for r in runs) for k in ("plain", "tls")}
        a["conn"], a["conn_fail"] = conn, fails
        agg[p["name"]] = a

        rows.append({
            "profile": p["name"], "distance_m": p["distance_m"], "rssi_dbm": p["rssi_dbm"],
            "netem": f"loss {p['loss_pct']}%, delay {p['delay_ms']}±{p['jitter_ms']} ms, rate {p['rate_mbit']} Mbit/s",
            "series": n,
            "throughput_mbit": fmt(*thr[:2]), "tcp_retransmits": fmt(*retr[:2], 0),
            "ping_loss_pct": fmt(*loss[:2], 1), "jitter_ms": fmt(*jit[:2], 1),
            "rtt_p99_ms_ci": fmt(*p99_series[:2], 1),
            "mqtt_q0_loss_pct": fmt(*a["q0_loss"][:2], 2), "mqtt_q1_loss_pct": fmt(*a["q1_loss"][:2], 2),
            "mqtt_q1_duplicates": a["q1_dup"],
        })
        for label, vals in (("ping RTT", rtt_all), ("MQTT QoS0", a["q0_lat"]),
                            ("MQTT QoS1", a["q1_lat"]), ("QoS1 PUBACK", a["q1_ack"])):
            prow.append({"profile": p["name"], "rssi_dbm": p["rssi_dbm"], "metric": label, "n": len(vals),
                         "mean_ms": round(float(np.mean(vals)), 1) if vals else None,
                         "p50_ms": round(pct(vals, 50), 1) if vals else None,
                         "p95_ms": round(pct(vals, 95), 1) if vals else None,
                         "p99_ms": round(pct(vals, 99), 1) if vals else None})
        for k in ("plain", "tls"):
            c = conn[k]
            crow.append({"profile": p["name"], "rssi_dbm": p["rssi_dbm"], "mode": "TCP+MQTT" if k == "plain" else "TCP+TLS+MQTT",
                         "n": len(c), "failures": fails[k],
                         "tcp_p50_ms": round(pct([x["tcp_ms"] for x in c], 50), 1) if c else None,
                         "tls_p50_ms": round(pct([x["tls_ms"] for x in c], 50), 1) if c else None,
                         "mqtt_p50_ms": round(pct([x["mqtt_ms"] for x in c], 50), 1) if c else None,
                         "total_p50_ms": round(pct([x["total_ms"] for x in c], 50), 1) if c else None,
                         "total_p95_ms": round(pct([x["total_ms"] for x in c], 95), 1) if c else None,
                         "total_max_ms": round(max(x["total_ms"] for x in c), 1) if c else None})

    for name, rws in (("summary.csv", rows), ("percentiles.csv", prow), ("connect.csv", crow)):
        with open(os.path.join(OUT, name), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=rws[0].keys())
            w.writeheader()
            w.writerows(rws)

    # ---------------------------------------------------------------- NFR
    lines = [f"NFR: {nfr}", ""]
    first_fail = None
    for p in profs:
        a = agg[p["name"]]
        q1_deliv = 100 - (a["q1_loss"][0] or 0)
        q1_p99 = pct(a["q1_lat"], 99) or float("inf")
        c95 = pct([x["total_ms"] for x in a["conn"]["tls"]], 95) or float("inf")
        thr = a["thr"][0] or 0
        checks = {
            f"QoS1 delivery {q1_deliv:.2f}% >= {nfr['mqtt_delivery_min_pct']}": q1_deliv >= nfr["mqtt_delivery_min_pct"],
            f"QoS1 p99 {q1_p99:.0f} ms <= {nfr['mqtt_p99_max_ms']}": q1_p99 <= nfr["mqtt_p99_max_ms"],
            f"connect p95 {c95:.0f} ms <= {nfr['connect_p95_max_ms']}": c95 <= nfr["connect_p95_max_ms"],
            f"throughput {thr:.2f} Mbit/s >= {nfr['throughput_min_mbit']}": thr >= nfr["throughput_min_mbit"],
        }
        ok = all(checks.values())
        if not ok and first_fail is None:
            first_fail = p
        lines.append(f"{p['name']} RSSI {p['rssi_dbm']} dBm: {'OK' if ok else 'FAIL'}")
        lines += [f"    [{'+' if v else '-'}] {k}" for k, v in checks.items()]
    lines.append("")
    lines.append(f"Система перестаёт удовлетворять NFR при RSSI <= {first_fail['rssi_dbm']} dBm ({first_fail['name']})"
                 if first_fail else "NFR выполняются на всех профилях")
    open(os.path.join(OUT, "nfr.txt"), "w", encoding="utf-8").write("\n".join(lines))
    print("\n".join(lines))

    # ---------------------------------------------------------------- графики
    rssi = [p["rssi_dbm"] for p in profs]

    def finish(fig, ax, name, title):
        ax.set_title(title, loc="left", fontsize=11, color=INK, pad=10)
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, name), facecolor="white")
        plt.close(fig)

    # 1. RSSI(d)
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    d = np.linspace(1, 32, 200)
    ax.plot(d, [rssi_at(x) for x in d], color=COLORS[0], label="модель: RSSI = −40 − 30·lg d")
    ax.plot([p["distance_m"] for p in profs], rssi, "o", color=COLORS[0], markersize=8,
            markeredgecolor="white", markeredgewidth=2, label="точки измерений")
    for p in profs:
        ax.annotate(f"{p['rssi_dbm']:.0f} dBm", (p["distance_m"], p["rssi_dbm"]),
                    textcoords="offset points", xytext=(6, 6), fontsize=9, color=INK2)
    ax.set_xlabel("Расстояние до точки доступа, м")
    ax.set_ylabel("RSSI, dBm")
    ax.legend(loc="upper right")
    finish(fig, ax, "fig1_rssi_distance.png", "RSSI в зависимости от расстояния")

    # 2. throughput(RSSI)
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    th = [agg[p["name"]]["thr"] for p in profs]
    ax.errorbar(rssi, [t[0] or 0 for t in th], yerr=[t[1] or 0 for t in th], color=COLORS[0],
                marker="o", markersize=7, capsize=4, markeredgecolor="white", markeredgewidth=1.5,
                label="TCP (iperf3), среднее ± 95 % ДИ")
    ax.plot(rssi, [p["rate_mbit"] for p in profs], "--", color=INK2, linewidth=1.2,
            label="полоса канала (netem rate)")
    for x, t in zip(rssi, th):
        ax.annotate(f"{t[0]:.2f}", (x, t[0]), textcoords="offset points", xytext=(6, 4), fontsize=9, color=INK2)
    ax.set_xlabel("RSSI, dBm")
    ax.set_ylabel("Throughput, Мбит/с")
    ax.set_yscale("log")
    ax.invert_xaxis()
    ax.legend(loc="lower left")
    finish(fig, ax, "fig2_throughput_rssi.png", "Пропускная способность в зависимости от RSSI")

    # 3. loss(RSSI)
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    series = [("ping (ICMP)", "loss", COLORS[0], "o"),
              ("MQTT QoS 0", "q0_loss", COLORS[1], "s"),
              ("MQTT QoS 1", "q1_loss", COLORS[2], "^")]
    for k, (label, key, col, mk) in enumerate(series):
        v = [agg[p["name"]][key] for p in profs]
        xs = [x + (k - 1) * 0.6 for x in rssi]          # небольшой сдвиг, чтобы совпадающие точки не закрывали друг друга
        ax.errorbar(xs, [x[0] or 0 for x in v], yerr=[x[1] or 0 for x in v], color=col, marker=mk,
                    markersize=7, capsize=4, markeredgecolor="white", markeredgewidth=1.5, label=label)
    ax.plot(rssi, [p["loss_pct"] for p in profs], ":", color=INK2, linewidth=1.2, label="заданные потери netem")
    ax.set_xlabel("RSSI, dBm")
    ax.set_ylabel("Потери, %")
    ax.invert_xaxis()
    ax.legend(loc="upper left")
    finish(fig, ax, "fig3_loss_rssi.png", "Потери пакетов и сообщений в зависимости от RSSI")

    # 4. latency CDF (ping RTT)
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    for i, p in enumerate(profs):
        v = np.sort(agg[p["name"]]["rtt"])
        if not len(v):
            continue
        y = np.arange(1, len(v) + 1) / len(v)
        ax.plot(v, y, color=COLORS[i], label=f"{p['rssi_dbm']:.0f} dBm ({p['distance_m']} м)")
        p99 = pct(list(v), 99)
        ax.plot([p99], [0.99], MARKERS[i], color=COLORS[i], markersize=7, markeredgecolor="white")
    ax.axhline(0.99, color=INK2, linewidth=0.8, linestyle=":")
    ax.text(0.0, 0.99, " p99", transform=ax.get_yaxis_transform(),
            va="bottom", fontsize=9, color=INK2)
    ax.set_xscale("log")
    ax.set_xlabel("RTT (ping), мс — логарифмическая шкала")
    ax.set_ylabel("Доля измерений ≤ x")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", title="RSSI")
    finish(fig, ax, "fig4_latency_cdf.png", "Функция распределения задержки (CDF)")

    # 5. MQTT QoS0 vs QoS1 latency CDF + время подключения
    fig, ax = plt.subplots(figsize=(6.4, 3.8))
    for i, p in enumerate(profs):
        for q, ls in ((0, ":"), (1, "-")):
            v = np.sort(agg[p["name"]][f"q{q}_lat"])
            if len(v):
                ax.plot(v, np.arange(1, len(v) + 1) / len(v), ls, color=COLORS[i], linewidth=1.6,
                        label=f"{p['rssi_dbm']:.0f} dBm" if q == 1 else None)
    ax.plot([], [], ":", color=INK2, label="QoS 0")
    ax.plot([], [], "-", color=INK2, label="QoS 1")
    ax.set_xscale("log")
    ax.set_xlabel("Задержка доставки MQTT (publish → subscriber), мс")
    ax.set_ylabel("Доля сообщений ≤ x")
    ax.legend(loc="lower right", ncol=2, fontsize=8)
    finish(fig, ax, "fig5_mqtt_latency_cdf.png", "Задержка доставки MQTT: QoS 0 и QoS 1")

    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    x = np.arange(len(profs))
    parts = [("TCP connect", "tcp_ms", COLORS[0]), ("TLS handshake", "tls_ms", COLORS[1]),
             ("MQTT CONNECT/CONNACK", "mqtt_ms", COLORS[2])]
    bottom = np.zeros(len(profs))
    for label, key, col in parts:
        vals = np.array([pct([c[key] for c in agg[p["name"]]["conn"]["tls"]], 50) or 0 for p in profs])
        ax.bar(x, vals, 0.6, bottom=bottom, color=col, edgecolor="white", linewidth=2, label=label)
        bottom += vals
    for xi, tot in zip(x, bottom):
        ax.annotate(f"{tot:.0f} мс", (xi, tot), textcoords="offset points", xytext=(0, 4),
                    ha="center", fontsize=9, color=INK)
    ax.set_xticks(x, [f"{p['rssi_dbm']:.0f} dBm" for p in profs])
    ax.set_ylabel("Медиана, мс")
    ax.legend(loc="upper left")
    finish(fig, ax, "fig6_connect_time.png", "Время подключения TCP + TLS + MQTT (медиана)")

    print(f"\nГотово: {OUT}/summary.csv, percentiles.csv, connect.csv, nfr.txt, fig1..fig6")


if __name__ == "__main__":
    main()
