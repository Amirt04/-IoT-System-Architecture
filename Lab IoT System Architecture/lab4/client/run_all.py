"""Все 8 проверок Lab 4 подряд + сводная таблица results/summary.md (~2–3 мин)."""
import json
import os
import subprocess
import sys

from common import RESULTS

CHECKS = ["check1_auth.py", "check2_acl.py", "check3_persistence.py", "check4_limits.py",
          "check5_bridge.py", "check6_sys.py", "check7_emqx.py", "check8_shared.py"]

only = sys.argv[1:]
for i, script in enumerate(CHECKS, 1):
    if only and str(i) not in only:
        continue
    subprocess.run([sys.executable, "-u", script])

rows = []
for i in range(1, 9):
    p = os.path.join(RESULTS, f"check{i}.json")
    if os.path.exists(p):
        r = json.load(open(p, encoding="utf-8"))
        rows.append((i, r["title"], r["passed"], r["total"]))
print("\n================ СВОДКА 8 ПРОВЕРОК ================")
lines = ["| № | Проверка | Пройдено | Итог |", "|---|---|---|---|"]
for i, title, p, t in rows:
    st = "PASS" if p == t else "FAIL"
    print(f" {i}. {title:<62} {p:>2}/{t:<2} {st}")
    lines.append(f"| {i} | {title} | {p}/{t} | {st} |")
with open(os.path.join(RESULTS, "summary.md"), "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print(f"\nПодробности: {RESULTS}/check*.json, сводка: {RESULTS}/summary.md")
