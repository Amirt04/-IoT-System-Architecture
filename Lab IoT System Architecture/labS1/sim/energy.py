"""Энергетическая модель устройства LoRaWAN класса A (SX1276 + малопотребляющий MCU, питание 3,3 В).

Цикл передачи:  [wake + опрос датчика] -> [TX, ToA(SF)] -> пауза 1 с -> [RX1, тот же SF]
                -> пауза до 2 с -> [RX2, SF12 / 869,525 МГц (DR0 EU868)] -> [sleep до следующего периода]
Если в окне нет преамбулы, радио держит приём только на время обнаружения преамбулы:
    t_rx = RX_SYMBOLS · Tsym(SF окна) + RX_MARGIN (запас на уход часов).
Токи — типовые значения из документации SX1276 и MCU класса STM32L0; это допущения модели,
их можно заменить замером (Nordic PPK II / INA219) — пункт 5 HW-варианта.
"""
from airtime import symbol_time, time_on_air

V = 3.3                 # В
I_SLEEP = 2e-6          # А — MCU stop + RTC + SX1276 sleep
I_WAKE = 3e-3           # А — MCU активен (опрос датчика, формирование кадра)
T_WAKE = 0.050          # с
I_TX = 44e-3            # А — SX1276, +14 дБм (PA_BOOST)
I_RX = 11.5e-3          # А — SX1276, приём LoRa 125 кГц
I_WAIT = 2e-6           # А — между TX и RX-окнами MCU спит по RTC
RX1_DELAY, RX2_DELAY = 1.0, 2.0
RX_SYMBOLS = 8          # символов на обнаружение преамбулы
RX_MARGIN = 0.020       # с
RX2_SF = 12

BATTERY_MAH = 2400      # 2×AA литиевые (Li-FeS2) ≈ 2,4–3 А·ч; берём 2400
USABLE = 0.85           # доля, доступная до напряжения отсечки / с учётом температуры
SHELF_LIFE_Y = 10       # срок, после которого ограничивает саморазряд/старение батареи


def rx_window(sf):
    return RX_SYMBOLS * symbol_time(sf) + RX_MARGIN


def cycle(sf, payload=20, rx_windows=True):
    """Заряд (Кл) по фазам одного цикла передачи и его длительность (с)."""
    toa = time_on_air(sf, payload)
    t_rx1, t_rx2 = (rx_window(sf), rx_window(RX2_SF)) if rx_windows else (0.0, 0.0)
    # RX1 открывается через 1 с после конца TX, RX2 — через 2 с; между ними MCU спит по RTC
    t_wait = (RX2_DELAY - t_rx1) if rx_windows else 0.0
    q = {
        "wake": I_WAKE * T_WAKE,
        "tx": I_TX * toa,
        "rx1": I_RX * t_rx1,
        "rx2": I_RX * t_rx2,
        "wait": I_WAIT * t_wait,
    }
    active = T_WAKE + toa + (RX2_DELAY + t_rx2 if rx_windows else 0.0)
    return q, active, toa


def budget(sf, period_s, payload=20, rx_windows=True):
    """Средний ток, доли энергии и срок службы батареи."""
    q, active, toa = cycle(sf, payload, rx_windows)
    q = dict(q)
    q["sleep"] = I_SLEEP * max(period_s - active, 0.0)
    q_total = sum(q.values())
    i_avg = q_total / period_s
    hours = BATTERY_MAH * USABLE / 1000 / i_avg
    years = hours / 24 / 365
    return {
        "sf": sf, "period_s": period_s, "toa_ms": toa * 1000,
        "q_msg_mC": (q_total - q["sleep"]) * 1000,
        "e_msg_mJ": (q_total - q["sleep"]) * V * 1000,
        "share": {k: v / q_total for k, v in q.items()},
        "i_avg_uA": i_avg * 1e6,
        "years_raw": years,
        "years": min(years, SHELF_LIFE_Y),
        "duty_cycle_pct": 100 * toa / period_s,
    }
