"""Эксперимент QoS: по 1000 сообщений с QoS 0/1/2 при потерях 0 %, 5 %, 20 % (tc netem в обе стороны).

Для каждой комбинации фиксируется: доставлено (уникальных seq), дубли, потери, задержка
(от publish до получения подписчиком; часы общие — оба клиента в одном контейнере), переподключения.
Издатель и подписчик — persistent-сессии (clean_session=False), keepalive 5 с: так ведёт себя
типичное IoT-устройство, и именно при переподключении QoS 1 повторяет неподтверждённые PUBLISH (флаг DUP).

Запуск:  python exp_qos.py                 (~6–10 мин)
         python exp_qos.py --n 300 --loss 0,20   (быстрая проверка)
Результат: /results/qos_results.csv, qos_results.json, fig_qos.png
"""
import argparse
import csv
import os
import threading
import time

import paho.mqtt.client as mqtt

from common import RESULTS, log, parse, pct, save_json, t_topic, uid
from common import BROKER, PORT
from netctl import Net

KEEPALIVE = 5


def run(net, qos, loss, n, rate, outage=0.0):
    topic = t_topic("lab", f"q{qos}-l{loss}-{uid('r')[-6:]}", "temperature")
    running = {"on": True}
    recv = {}                       # seq -> задержка первой копии, мс
    stat = {"dup": 0, "dup_flag": 0, "sub_disc": 0, "pub_disc": 0, "last_rx": time.time()}
    lock = threading.Lock()
    sub_ready = threading.Event()

    def on_msg(c, u, m):
        now = time.time()
        p = parse(m.payload)
        if not p:
            return
        with lock:
            stat["last_rx"] = now
            if m.dup:
                stat["dup_flag"] += 1
            if p["seq"] in recv:
                stat["dup"] += 1
            else:
                recv[p["seq"]] = (now - p["ts"]) * 1000

    sub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("sub"), clean_session=False)
    sub.on_message = on_msg
    sub.on_connect = lambda c, u, f, rc, p: c.subscribe(topic, qos=qos)
    sub.on_subscribe = lambda *a: sub_ready.set()
    sub.on_disconnect = lambda *a: running["on"] and stat.__setitem__("sub_disc", stat["sub_disc"] + 1)
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=uid("pub"), clean_session=False)
    pub.on_disconnect = lambda *a: running["on"] and stat.__setitem__("pub_disc", stat["pub_disc"] + 1)
    for c in (sub, pub):
        c.reconnect_delay_set(1, 4)
        c.connect(BROKER, PORT, keepalive=KEEPALIVE)
        c.loop_start()
    sub_ready.wait(15)
    time.sleep(0.5)

    net.set_loss(loss)
    t0 = time.time()
    offline_pub = 0
    period, nxt = 1.0 / rate, time.monotonic()
    outage_thread = None
    for seq in range(n):
        if outage and seq == n // 2:
            outage_thread = net.outage(outage, loss)      # обрыв Wi-Fi посреди передачи
        info = pub.publish(topic, f'{{"seq":{seq},"ts":{time.time():.6f},"v":21.5}}', qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            offline_pub += 1
        nxt += period
        time.sleep(max(0.0, nxt - time.monotonic()))
    t_sent = time.time()

    if outage_thread:
        outage_thread.join()
    # ждём доставки под потерями: до получения всех или 30 с тишины (предел 300 с)
    idle = 10 if qos == 0 else 30
    while len(recv) < n and time.time() - stat["last_rx"] < idle and time.time() - t_sent < 300:
        time.sleep(0.2)
    t_deliver = time.time()
    net.clear()
    # потери сняты — дожидаемся хвоста очереди и запоздавших копий (дублей после переподключения)
    t_clear = time.time()
    while (len(recv) < n or time.time() - max(stat["last_rx"], t_clear) < 5) \
            and time.time() - max(stat["last_rx"], t_clear) < 15 and time.time() - t_clear < 120:
        time.sleep(0.2)
    running["on"] = False
    for c in (pub, sub):
        c.loop_stop()
        c.disconnect()

    lat = list(recv.values())
    return {
        "qos": qos, "loss_pct": loss, "outage_s": outage, "sent": n, "delivered": len(recv),
        "delivered_pct": round(100 * len(recv) / n, 2), "lost": n - len(recv),
        "duplicates": stat["dup"], "dup_flag_seen": stat["dup_flag"],
        "lat_mean_ms": round(sum(lat) / len(lat), 1) if lat else None,
        "lat_p50_ms": round(pct(lat, 50), 1) if lat else None,
        "lat_p95_ms": round(pct(lat, 95), 1) if lat else None,
        "lat_p99_ms": round(pct(lat, 99), 1) if lat else None,
        "lat_max_ms": round(max(lat), 1) if lat else None,
        "pub_reconnects": stat["pub_disc"], "sub_reconnects": stat["sub_disc"],
        "published_while_offline": offline_pub,
        "send_s": round(t_sent - t0, 1), "deliver_s": round(t_deliver - t0, 1),
        "total_s": round(time.time() - t0, 1),
    }


def plot(rows):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return
    colors = {0: "#2a78d6", 1: "#eb6834", 2: "#1baf7a"}      # палитра проверена на различимость (CVD)
    conds = []
    for r in rows:
        k = (r["loss_pct"], r["outage_s"])
        if k not in conds:
            conds.append(k)
    labels = [f"{l} %" + (f"\n+обрыв PUBACK\n{o:g} с" if o else "") for l, o in conds]
    fig, axs = plt.subplots(1, 3, figsize=(10.5, 3.6))
    w = 0.27
    panels = [("delivered_pct", "Доставлено, %", "lin"), ("duplicates", "Дубли, шт.", "lin"),
              ("lat_mean_ms", "Средняя задержка, мс (лог. шкала)", "log")]
    for ax, (key, title, scale) in zip(axs, panels):
        for i, q in enumerate((0, 1, 2)):
            vals = [next(((r[key] or 0) for r in rows if r["qos"] == q and (r["loss_pct"], r["outage_s"]) == k), 0)
                    for k in conds]
            xs = [j + (i - 1) * w for j in range(len(conds))]
            ax.bar(xs, vals, w, color=colors[q], edgecolor="white", linewidth=1.5, label=f"QoS {q}")
            for x, v in zip(xs, vals):
                show = (key == "delivered_pct" and v < 100) or (key == "duplicates" and v > 0) or key == "lat_mean_ms"
                if show:
                    txt = f"{v:.0f}" if v >= 10 or key != "lat_mean_ms" else f"{v:.1f}"
                    ax.annotate(txt, (x, v), textcoords="offset points", xytext=(0, 2), ha="center",
                                fontsize=6.5, color="#0b0b0b", rotation=90 if key == "lat_mean_ms" else 0)
        ax.set_xticks(range(len(conds)), labels, fontsize=7.5)
        ax.set_title(title, loc="left", fontsize=10, color="#0b0b0b")
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", color="#e4e3df", linewidth=0.6)
        ax.set_axisbelow(True)
        if scale == "log":
            ax.set_yscale("log")
            ax.set_ylim(top=ax.get_ylim()[1] * 8)
    axs[0].set_ylim(0, 112)
    axs[1].set_ylim(0, max(5, max(r["duplicates"] for r in rows) * 1.25))
    axs[0].text(0.0, 104, "столбец без подписи = 100 %", fontsize=7, color="#52514e")
    h, l = axs[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=3, frameon=False, fontsize=9, bbox_to_anchor=(0.5, 1.0))
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    fig.savefig(os.path.join(RESULTS, "fig_qos.png"), dpi=150, facecolor="white")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--rate", type=float, default=50, help="сообщений в секунду")
    ap.add_argument("--loss", default="0,5,20")
    ap.add_argument("--qos", default="0,1,2")
    ap.add_argument("--outage", type=float, default=12,
                    help="доп. условие: макс. потери + обрыв обратного канала на N секунд посреди передачи (0 = нет)")
    a = ap.parse_args()
    net = Net()
    rows = []
    try:
        losses = [int(x) for x in a.loss.split(",")]
        conds = [(l, 0.0) for l in losses] + ([(max(losses), a.outage)] if a.outage else [])
        for loss, outage in conds:
            for qos in [int(x) for x in a.qos.split(",")]:
                extra = f" + обрыв обратного канала {outage:g} с" if outage else ""
                log(f"QoS {qos}, потери {loss} %{extra}: отправляю {a.n} сообщений ({a.rate:g}/с)")
                r = run(net, qos, loss, a.n, a.rate, outage)
                rows.append(r)
                log(f"  доставлено {r['delivered']}/{r['sent']}, дублей {r['duplicates']}, "
                    f"задержка средн. {r['lat_mean_ms']} мс, p99 {r['lat_p99_ms']} мс, "
                    f"переподключений pub/sub {r['pub_reconnects']}/{r['sub_reconnects']}")
    finally:
        net.close()
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, "qos_results.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    save_json("qos_results.json", rows)
    plot(rows)
    print("\n===== QoS × потери =====")
    print(f"{'QoS':>3} {'условия':>14} {'доставлено':>11} {'дублей':>7} {'средн.мс':>9} {'p99 мс':>8} {'reconn':>7}")
    for r in rows:
        cond = f"{r['loss_pct']}%" + (f"+обрыв {r['outage_s']:g}с" if r["outage_s"] else "")
        print(f"{r['qos']:>3} {cond:>14} {r['delivered']:>6}/{r['sent']:<4} {r['duplicates']:>7} "
              f"{r['lat_mean_ms']!s:>9} {r['lat_p99_ms']!s:>8} {r['pub_reconnects'] + r['sub_reconnects']:>7}")
    print(f"\nCSV: {RESULTS}/qos_results.csv, график: {RESULTS}/fig_qos.png")


if __name__ == "__main__":
    main()
