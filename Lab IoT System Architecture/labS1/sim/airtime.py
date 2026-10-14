"""Время в эфире (Time on Air) пакета LoRa — формула Semtech AN1200.13 / SX1276 datasheet §4.1.1.6.

Аналог библиотеки lorawan-airtime: ToA зависит от SF, полосы BW, кодовой скорости CR,
длины PHY-пакета и включённой оптимизации низкой скорости (LDRO, обязательна при Tsym > 16 мс).
"""
import math

LORAWAN_OVERHEAD = 13   # MHDR 1 + DevAddr 4 + FCtrl 1 + FCnt 2 + FPort 1 + MIC 4, байт


def symbol_time(sf, bw=125e3):
    return (2 ** sf) / bw


def time_on_air(sf, payload_bytes, bw=125e3, cr=1, preamble=8, explicit_header=True,
                crc=True, lorawan=True):
    """Возвращает ToA в секундах. payload_bytes — полезная нагрузка приложения (FRMPayload)."""
    pl = payload_bytes + (LORAWAN_OVERHEAD if lorawan else 0)
    tsym = symbol_time(sf, bw)
    de = 1 if tsym > 0.016 else 0          # LDRO: SF11/SF12 при 125 кГц
    ih = 0 if explicit_header else 1
    t_preamble = (preamble + 4.25) * tsym
    num = 8 * pl - 4 * sf + 28 + 16 * int(crc) - 20 * ih
    n_payload = 8 + max(math.ceil(num / (4 * (sf - 2 * de))) * (cr + 4), 0)
    return t_preamble + n_payload * tsym


if __name__ == "__main__":
    # проверка по LoRa Calculator (Semtech): SF7, BW125, CR4/5, 10 байт PHY, CRC, явный заголовок -> 41,22 мс
    print(f"self-test SF7 PL=10: {time_on_air(7, 10, lorawan=False) * 1000:.2f} ms (эталон 41.22)")
    for sf in range(7, 13):
        print(f"SF{sf}: Tsym={symbol_time(sf) * 1000:.3f} ms, ToA(20 B app) = {time_on_air(sf, 20) * 1000:.1f} ms")
