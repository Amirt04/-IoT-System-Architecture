"""Профили канала для SW-режима Lab 2.

Расстояние -> RSSI по логарифмической модели затухания внутри помещения:
    RSSI(d) = P0 - 10 * n * log10(d),  P0 = -40 dBm на 1 м, n = 3.0
RSSI -> параметры tc netem. Чем ниже RSSI (SNR), тем ниже скорость модуляции
(MCS) -> меньше полоса; больше повторов на MAC-уровне -> выше задержка и
джиттер; часть кадров теряется окончательно -> остаточные потери.
Уровни потерь 0/1/3/10/20 % взяты из задания (вариант «5 уровней потерь»).
"""
import math

P0_DBM = -40.0      # RSSI на расстоянии 1 м
PATH_LOSS_N = 3.0   # показатель затухания (офис/квартира: 2.7–3.5)


def rssi_at(distance_m: float) -> float:
    return P0_DBM - 10 * PATH_LOSS_N * math.log10(distance_m)


#           name    d,м  loss,%  delay,мс  jitter,мс  rate,Мбит/с
_TABLE = [("d01",   1,    0.0,     2,        1,        50),
          ("d05",   5,    1.0,     4,        2,        30),
          ("d10",  10,    3.0,     8,        4,        15),
          ("d20",  20,   10.0,    20,       10,         5),
          ("d30",  30,   20.0,    40,       20,         2)]

PROFILES = [
    {"name": n, "distance_m": d, "rssi_dbm": round(rssi_at(d), 1), "loss_pct": l,
     "delay_ms": dl, "jitter_ms": j, "rate_mbit": r}
    for n, d, l, dl, j, r in _TABLE
]


def netem_args(p):
    """Аргументы tc для профиля (применяются на исходящем трафике клиента = uplink устройства)."""
    args = ["delay", f"{p['delay_ms']}ms"]
    if p["jitter_ms"]:
        args.append(f"{p['jitter_ms']}ms")
    if p["loss_pct"]:
        args += ["loss", f"{p['loss_pct']}%"]
    args += ["rate", f"{p['rate_mbit']}mbit"]
    return args
