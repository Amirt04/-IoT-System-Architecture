"""Графики и сводка Lab 6:  python plots.py  ->  /results/plots/*.png, /results/summary.md"""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = os.getenv("RESULTS", "/results")
OUT = os.path.join(RESULTS, "plots")
os.makedirs(OUT, exist_ok=True)
R = json.load(open(os.path.join(RESULTS, "lab6.json"), encoding="utf-8"))
plt.rcParams.update({"figure.dpi": 130, "font.size": 9, "axes.grid": True, "grid.alpha": .3})
COL = {"wide_raw": "#e67e22", "narrow_raw": "#2e86de", "wide_comp": "#f5b041", "narrow_comp": "#85c1e9",
       "narrow_comp2": "#1b4f72", "cagg": "#27ae60"}
NAMES = {"wide_raw": "широкая", "narrow_raw": "узкая", "wide_comp": "широкая + сжатие",
         "narrow_comp": "узкая + сжатие (device)", "narrow_comp2": "узкая + сжатие (device, metric)", "cagg": "агрегаты CAGG"}
QN = {"q1_last": "Q1 последнее значение", "q2_day_avg": "Q2 среднее за сутки", "q3_anomalies": "Q3 аномалии за месяц",
      "q4_device_year": "Q4 устройство за год"}


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, name))
    plt.close(fig)
    print("  ", os.path.join(OUT, name))


L, C, S2 = R["load"], R.get("compress", {}), R.get("seg2")
sz = {"wide_raw": (L["telemetry_wide"]["table_mb"], L["telemetry_wide"]["index_mb"], L["telemetry_wide"]["toast_mb"]),
      "narrow_raw": (L["telemetry_narrow"]["table_mb"], L["telemetry_narrow"]["index_mb"], L["telemetry_narrow"]["toast_mb"])}
if C:
    for k, t in (("wide_comp", "telemetry_wide"), ("narrow_comp", "telemetry_narrow")):
        sz[k] = (C[t]["after_table"], C[t]["after_index"], C[t]["after_toast"])
if S2:
    sz["narrow_comp2"] = (S2["after_table"], S2["after_index"], S2["after_toast"])

fig, ax = plt.subplots(figsize=(7.5, 3.6))
keys = list(sz)
y = range(len(keys))
left = [0] * len(keys)
for i, (part, col) in enumerate((("данные", None), ("индексы", None), ("TOAST", None))):
    vals = [sz[k][i] for k in keys]
    ax.barh(y, vals, left=left, color=[COL[k] for k in keys], alpha=[1, .55, .8][i], edgecolor="white",
            label=part, hatch=[None, "//", ".."][i])
    left = [a + b for a, b in zip(left, vals)]
for i, k in enumerate(keys):
    ax.text(left[i], i, f"  {left[i]:,.0f} МБ".replace(",", " "), va="center")
ax.set_yticks(list(y), [NAMES[k] for k in keys])
ax.invert_yaxis()
ax.set_xlim(0, max(left) * 1.18)
ax.set(xlabel="МБ", title=f"Объём хранения: {L['telemetry_wide']['rows']:,} замеров × 4 метрики".replace(",", " "))
ax.legend(loc="lower right")
save(fig, "sizes.png")

cfg = {"wide_raw": R["bench_raw"]["wide"], "narrow_raw": R["bench_raw"]["narrow"]}
if "bench_comp" in R:
    cfg.update(wide_comp=R["bench_comp"]["wide"], narrow_comp=R["bench_comp"]["narrow"])
if S2:
    cfg["narrow_comp2"] = S2["bench"]
if "cagg" in R:
    cfg["cagg"] = R["cagg"]["bench"]
fig, axs = plt.subplots(1, 4, figsize=(11, 3.8))
for ax, qn in zip(axs, QN):
    ks = [k for k in cfg if qn in cfg[k]]
    v = [cfg[k][qn]["median_ms"] for k in ks]
    ax.bar(range(len(ks)), v, color=[COL[k] for k in ks])
    for i, x in enumerate(v):
        ax.text(i, x, f"{x:.1f}" if x < 100 else f"{x:.0f}", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(range(len(ks)), [NAMES[k] for k in ks], rotation=60, ha="right", fontsize=7)
    ax.set_yscale("log")
    ax.set_title(QN[qn])
axs[0].set_ylabel("медиана, мс (лог.)")
save(fig, "queries.png")

if "cold" in R and R["cold"]["tables"]:
    fig, ax = plt.subplots(figsize=(7.5, 3.6))
    items = [("pg_raw_mb", "PostgreSQL без сжатия"), ("pg_compressed_mb", "PostgreSQL сжато"), ("csv_mb", "CSV"),
             ("parquet_snappy_mb", "Parquet snappy"), ("parquet_zstd_mb", "Parquet zstd")]
    tabs = list(R["cold"]["tables"])
    w = .38
    for j, t in enumerate(tabs):
        d = R["cold"]["tables"][t]
        vals = [d[k] or 0 for k, _ in items]
        xs = [i + (j - .5) * w for i in range(len(items))]
        ax.bar(xs, vals, w, color=COL["wide_raw" if "wide" in t else "narrow_raw"],
               label=f"{'широкая' if 'wide' in t else 'узкая'}: {d['rows']:,} строк".replace(",", " "))
        for x, v in zip(xs, vals):
            ax.text(x, v, f"{v:.1f}", ha="center", va="bottom", fontsize=7)
    ax.set_xticks(range(len(items)), [n.replace(" ", "\n", 1) for _, n in items])
    first = R["cold"]["tables"][tabs[0]]
    ax.set(ylabel="МБ", title=f"Холодный срез {str(first['from'])[:10]} … {str(first['to'])[:10]}: где он меньше")
    ax.legend()
    save(fig, "cold_tier.png")

md = ["# Lab 6 — сводка", "", "| Конфигурация | Объём, МБ | Q1, мс | Q2, мс | Q3, мс | Q4, мс |", "|---|---|---|---|---|---|"]
for k in cfg:
    tot = sum(sz[k]) if k in sz else sum(R["cagg"][v]["size_mb"] for v in ("cagg_1m", "cagg_1h", "cagg_1d"))
    md.append(f"| {NAMES[k]} | {tot:.0f} | " + " | ".join(
        str(cfg[k][qn]["median_ms"]) if qn in cfg[k] else "–" for qn in QN) + " |")
if C:
    md += ["", "Коэффициент сжатия: " + ", ".join(f"{t} {C[t]['ratio']}×" for t in ("telemetry_wide", "telemetry_narrow"))
           + (f", narrow (device, metric) {S2['ratio']}×" if S2 else "")]
with open(os.path.join(RESULTS, "summary.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(md) + "\n")
print("\n".join(md))
