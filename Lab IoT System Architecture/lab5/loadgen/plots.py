"""Графики и сводка по результатам run_lab5.py:  python plots.py  ->  /results/plots/*.png, /results/summary.md"""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = os.getenv("RESULTS", "/results")
OUT = os.path.join(RESULTS, "plots")
os.makedirs(OUT, exist_ok=True)
plt.rcParams.update({"figure.dpi": 130, "font.size": 9, "axes.grid": True, "grid.alpha": .3})
C = {"in": "#7f8c8d", "thr": "#c0392b", "p50": "#2e86de", "p95": "#e67e22", "p99": "#c0392b",
     "none": "#c0392b", "inflight": "#2e86de", "throttle": "#27ae60"}


def load(name):
    p = os.path.join(RESULTS, f"{name}.json")
    return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else None


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, name))
    plt.close(fig)
    print("  ", os.path.join(OUT, name))


def lat(r, p):
    v = (r.get("latency") or {}).get(str(p))
    return v if v is not None else float("nan")


md = ["# Lab 5 — сводка результатов", ""]
loads = load("loads") or []
sat = load("saturation")
runs = loads + (sat["runs"] if sat else [])
runs = sorted(runs, key=lambda r: r["target_rate"])

if runs:
    x = [r["target_rate"] for r in runs]
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(x, x, ":", color="#bbb", label="идеал: запись = нагрузка")
    ax.plot(x, [r["achieved_rate"] for r in runs], "o-", color=C["in"], label="опубликовано устройствами")
    ax.plot(x, [r["throughput"] for r in runs], "s-", color=C["thr"], label="записано в хранилище")
    if sat and sat.get("first_saturated_load"):
        ax.axvline(sat["first_saturated_load"], color=C["thr"], ls="--", alpha=.5)
        ax.axhline(sat["capacity_msg_s"], color=C["thr"], ls=":", alpha=.5)
        ax.text(sat["first_saturated_load"], ax.get_ylim()[1] * .05, "  насыщение", color=C["thr"])
    ax.set(xlabel="нагрузка, сообщ./с (устройства × частота)", ylabel="сообщ./с", title="Пропускная способность")
    ax.legend()
    save(fig, "throughput_vs_load.png")

    fig, ax = plt.subplots(figsize=(7, 4))
    for p in (50, 95, 99):
        ax.plot(x, [lat(r, p) for r in runs], "o-", color=C[f"p{p}"], label=f"p{p}")
    ax.set_yscale("log")
    ax.set(xlabel="нагрузка, сообщ./с", ylabel="задержка, с (лог.)", title="Сквозная задержка (устройство → commit)")
    ax.legend()
    save(fig, "latency_vs_load.png")

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(x, [r["cpu_avg"] for r in runs], "o-", color=C["thr"], label="CPU ingest, % ядра")
    ax.set(xlabel="нагрузка, сообщ./с", ylabel="CPU, %", title="Ресурсы ingest-сервиса")
    ax2 = ax.twinx()
    ax2.plot(x, [r["rss_max"] for r in runs], "s--", color=C["p50"], label="RAM (RSS max), МБ")
    ax2.set_ylabel("RAM, МБ")
    ax2.grid(False)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left")
    save(fig, "cpu_ram_vs_load.png")

    fig, ax = plt.subplots(figsize=(7, 4))
    cmap = plt.get_cmap("viridis")
    for i, r in enumerate(runs):
        s = [p for p in r["series"] if "queue_depth" in p]
        ax.plot([p["t"] for p in s], [p["queue_depth"] + p["broker_queue"] for p in s],
                color=cmap(i / max(1, len(runs) - 1)), label=f"{r['target_rate']:g}/с{' *' if r['saturated'] else ''}")
    ax.set(xlabel="время от старта, с", ylabel="сообщений в очереди", title="Глубина очереди (сервис + брокер); * — насыщение")
    ax.legend(fontsize=7, ncol=2)
    save(fig, "queue_vs_time.png")

    md += ["## Нагрузочные прогоны (шаги 4–5)", "",
           "| Нагрузка, сообщ./с | Устр. × Гц | Опубл., /с | Записано, /с | p50, с | p95, с | p99, с | Очередь max | Рост очереди, /с | CPU, % | RAM, МБ | Потери | Насыщ. |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in runs:
        md.append(f"| {r['target_rate']:g} | {r['devices']}×{r['rate']:g} | {r['achieved_rate']} | {r['throughput']} | "
                  f"{lat(r, 50)} | {lat(r, 95)} | {lat(r, 99)} | {r['max_queue'] + r['max_broker_queue']} | "
                  f"{r['queue_slope'] + r['broker_queue_slope']:+.0f} | {r['cpu_avg']} | {r['rss_max']} | {r['lost']} | "
                  f"{'да' if r['saturated'] else 'нет'} |")
    if sat:
        md += ["", f"**Ёмкость конвейера ≈ {sat['capacity_msg_s']:.0f} сообщ./с**; последняя устойчивая нагрузка "
               f"{sat['last_ok_load']}, первая с неограниченным ростом очереди {sat['first_saturated_load']} сообщ./с.", ""]
    st = max(runs, key=lambda r: r["throughput"]).get("stage_seconds") or {}
    if st:
        tot = sum(st.values())
        md += ["Время по этапам (самый нагруженный прогон): " +
               ", ".join(f"{k} {v:.1f} с ({v / tot * 100:.0f} %)" for k, v in st.items()), ""]
    md += ["Трафик на брокер: " + ", ".join(f"{r['target_rate']:g}/с → {r['bytes_in_per_s'] / 1024:.0f} КБ/с "
                                             f"({r['bytes_per_msg']} Б/сообщ.)" for r in loads), ""]

bp = load("backpressure")
if bp:
    fig, axs = plt.subplots(3, 1, figsize=(7, 8), sharex=True)
    for r in bp["runs"]:
        m = r["cfg"]["mode"]
        s = [p for p in r["series"] if "queue_depth" in p]
        t = [p["t"] for p in s]
        axs[0].plot(t, [p["queue_depth"] for p in s], color=C[m], label=f"{m}: в сервисе")
        axs[0].plot(t, [p["broker_queue"] for p in s], color=C[m], ls="--", label=f"{m}: в брокере")
        axs[1].plot(t, [p["rss_mb"] for p in s], color=C[m], label=m)
        raw = [0] + [(b["stored"] - a["stored"]) / (b["t"] - a["t"]) for a, b in zip(s, s[1:])]
        rate = [sum(raw[max(0, i - 2):i + 1]) / len(raw[max(0, i - 2):i + 1]) for i in range(len(raw))]  # скольз. 3 с
        axs[2].plot(t, rate, color=C[m], label=f"{m}: запись")
        src = r.get("source_rate_series") or []
        axs[2].plot(range(len(src)), src, color=C[m], ls=":", alpha=.7, label=f"{m}: источники")
    axs[0].set(ylabel="сообщений", title=f"Перегрузка {bp['load']:.0f} сообщ./с (≈{bp['overload']}× ёмкости): очередь")
    axs[1].set(ylabel="RSS ingest, МБ", title="Память сервиса")
    axs[2].set(ylabel="сообщ./с", xlabel="время от старта, с", title="Темп записи (скольз. среднее 3 с) и темп источников")
    for ax in axs:
        ax.legend(fontsize=7, ncol=3)
    save(fig, "backpressure.png")
    md += ["## Backpressure (шаг 6)", "", f"Нагрузка {bp['load']:.0f} сообщ./с ≈ {bp['overload']}× ёмкости.", "",
           "| Режим | Опубл., /с | Записано, /с | p50, с | p99, с | Очередь в сервисе max | В брокере max | RAM max, МБ | Потери | Dropped брокером | Упал |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in bp["runs"]:
        md.append(f"| {r['cfg']['mode']} | {r['achieved_rate']} | {r['throughput']} | {lat(r, 50)} | {lat(r, 99)} | "
                  f"{r['max_queue']} | {r['max_broker_queue']:.0f} | {r['rss_max']} | {r['lost']} | {r['broker_dropped']:.0f} | "
                  f"{'да' if r['crashed'] else 'нет'} |")
    md.append("")

bt = load("batching")
if bt:
    rs = bt["runs"]
    labels = [str(r["batch"]) for r in rs]
    fig, axs = plt.subplots(1, 2, figsize=(9, 3.8))
    w = .25
    for i, p in enumerate((50, 95, 99)):
        axs[0].bar([k + (i - 1) * w for k in range(len(rs))], [lat(r["latency_run"], p) for r in rs], w,
                   color=C[f"p{p}"], label=f"p{p}")
    axs[0].set_xticks(range(len(rs)), labels)
    axs[0].set_yscale("log")
    axs[0].set(xlabel="max_batch_size", ylabel="задержка, с (лог.)",
               title=f"Задержка при {bt['load_devices']} сообщ./с")
    axs[0].legend()
    axs[1].bar(labels, [r["drain"]["throughput"] for r in rs], color=C["thr"])
    for k, r in enumerate(rs):
        axs[1].text(k, r["drain"]["throughput"], f"{r['drain']['throughput']:.0f}", ha="center", va="bottom")
    axs[1].set(xlabel="max_batch_size", ylabel="сообщ./с", title="Макс. пропускная способность")
    save(fig, "batching.png")
    md += ["## Эффект батчинга (шаг 7)", "",
           f"| max_batch_size | Средний батч | p50, с | p95, с | p99, с | Макс. пропускная, сообщ./с | Время записи в БД, с |",
           "|---|---|---|---|---|---|---|"]
    for r in rs:
        lr = r["latency_run"]
        md.append(f"| {r['batch']} | {(lr.get('batch') or {}).get('avg')} | {lat(lr, 50)} | {lat(lr, 95)} | {lat(lr, 99)} | "
                  f"{r['drain']['throughput']} | {(r['drain'].get('stage_seconds') or {}).get('db_write')} |")
    md.append("")

with open(os.path.join(RESULTS, "summary.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(md) + "\n")
print("\n".join(md))
