# Lab 3 — MQTT Publisher/Subscriber: топики, wildcards, QoS, retained, LWT

```
[publisher × N] ──► [mosquitto] ──► [subscriber: wildcard]
                        │
               [lab3-netem: tc netem + tcpdump -> captures/*.pcap -> Wireshark]
```

## Схема топиков
```
iot/v1/<site>/<device_id>/telemetry/<metric>   metric: temperature | humidity | pressure | motion
iot/v1/<site>/<device_id>/status               online / offline (retained, offline = LWT)
iot/v1/<site>/<device_id>/config               конфигурация (retained)
```
Подписки: `iot/v1/+/+/telemetry/temperature`, `iot/v1/astana/+/telemetry/+`, `iot/v1/#`.

## Запуск
```
docker compose up -d --build
docker exec lab3-client python subscriber.py --duration 40        # терминал 1
docker exec lab3-client python publisher.py --devices 3 --count 15 # терминал 2
docker exec lab3-client python exp_retained.py
docker exec lab3-client python exp_lwt.py
docker exec lab3-client python exp_capture.py     # -> captures/mqtt.pcap, открыть в Wireshark, фильтр: mqtt
docker exec lab3-client python exp_qos.py         # ~10–15 мин, -> results/qos_results.csv, fig_qos.png
```
`python run_all.py` — все эксперименты подряд.

## Файлы
| Файл | Назначение |
|---|---|
| `client/publisher.py` | N устройств × 4 метрики с `seq`, LWT |
| `client/subscriber.py` | wildcard-подписки, подсчёт, пропуски и дубли по `seq` |
| `client/exp_qos.py` | 1000 сообщений × QoS 0/1/2 × потери 0/5/20 % + обрыв обратного канала |
| `client/exp_retained.py` | подписка после публикации retained, удаление retained |
| `client/exp_lwt.py`, `lwt_victim.py` | `kill -9` и время появления `offline` |
| `client/exp_capture.py`, `pcap_mqtt.py` | захват tcpdump, разбор CONNECT/PUBLISH/PUBACK, накладные расходы |
| `client/netctl.py`, `agent/agent.py` | tc netem с обеих сторон (клиент и брокер), захват на брокере |

Примечание: в `mosquitto.conf` стоит `max_inflight_messages 0`. При значении по умолчанию (20)
mosquitto 2.0.x после переподключения подписчика под потерями переставал доставлять QoS 2.
