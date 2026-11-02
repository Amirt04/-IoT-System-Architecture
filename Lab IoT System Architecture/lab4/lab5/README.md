# Lab 5 — Telemetry Pipeline: батчинг, backpressure, точка насыщения

```
[simulator × N] ──MQTT QoS1──► [mosquitto] ──► [ingest-service (asyncio, aiomqtt)]
                                                   │ validate (JSON Schema) → enrich → batch
                                                   ▼
                                     [очередь записи] ──► [SQLite /data/telemetry.db]
                                                   │
                                     [/metrics] ──► [Prometheus :9090]
```

## Состав
| Каталог | Что внутри |
|---|---|
| `ingest/app.py` | сервис приёма: aiomqtt (MQTT 5), валидация `schema/telemetry.schema.json`, обогащение метаданными устройства, батч `max_batch_size=500 OR max_delay=2s`, запись в SQLite (WAL, `synchronous=FULL`), FastAPI `/metrics`, `/stats`, `/config`, `/reset`, `/pause`, `/resume` |
| `loadgen/simulator.py` | N устройств, у каждого своё MQTT-подключение, QoS 1, ~0,5 % заведомо невалидных сообщений |
| `loadgen/run_lab5.py` | все эксперименты: `loads`, `saturation`, `backpressure`, `batching` |
| `loadgen/plots.py` | графики `results/plots/*.png` и сводка `results/summary.md` |
| `mosquitto/mosquitto.conf` | `max_queued_messages 100000` — буфер брокера, `sys_interval 1` |
| `prometheus/prometheus.yml` | сбор `ingest:8000/metrics` раз в 2 с |

Ingest ограничен **1 CPU и 768 МБ** (`docker-compose.yml`), чтобы точка насыщения была воспроизводимой.

## Метрики
`messages_received_total`, `messages_invalid_total{reason}`, `messages_stored_total`, `batch_size` (гистограмма),
`processing_latency_seconds` (гистограмма, ts устройства → commit), `broker_wait_seconds`, `queue_depth`,
`inflight_messages`, `backpressure_factor`, `stage_seconds_total{stage}` (parse / validate / enrich / db_write),
`ingest_cpu_percent`, `ingest_rss_bytes`.

## Режимы перегрузки (`POST /config {"mode": ...}`)
| Режим | Поведение |
|---|---|
| `none` | PUBACK сразу при приёме, очереди без ограничений — память растёт до OOM |
| `inflight` | PUBACK только после commit в БД, окно MQTT 5 **Receive Maximum = 1000**, очередь записи ≤ 2 батчей; излишек ждёт в брокере |
| `throttle` | `inflight` + AIMD-сигнал источникам: retained `iot/v1/ctrl/backpressure {"factor"}` |

## Запуск
```
docker compose up -d --build
docker exec lab5-loadgen python run_lab5.py            # все сценарии, ~25 мин
docker exec lab5-loadgen python run_lab5.py loads      # или по одному
docker exec lab5-loadgen python plots.py               # графики + summary.md
docker compose run --rm capture                         # 30 с захвата для Wireshark -> captures/
```
Prometheus: http://localhost:9090 — например `rate(messages_stored_total[10s])`, `queue_depth`,
`histogram_quantile(0.99, rate(processing_latency_seconds_bucket[30s]))`.
