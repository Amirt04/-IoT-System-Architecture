# Lab 4 — MQTT Broker: конфигурация и возможности

```
[devices] ──► [mosquitto A] ══bridge══► [mosquitto B]        [EMQX node1 ⇄ node2]
                 │ persistence, ACL, limits                     кластер, дашборд, $share
                 └────────── metrics ($SYS) ─────────┘
```

## Конфигурация (`config/`)
| Файл | Что настроено |
|---|---|
| `broker-a.conf` | `allow_anonymous false`, password_file, acl_file, `persistence true`, `persistent_client_expiration 7d`, лимиты `max_inflight_messages 20`, `max_queued_messages 100`, `message_size_limit 1024`, `sys_interval 5`, мост `bridge-to-b` |
| `broker-b.conf` | принимающая сторона моста, свои пользователи и ACL |
| `acl_a`, `acl_b` | права по схеме `iot/v1/<site>/<device>/{telemetry,status,config,cmd}` |
| `users.txt` | пользователи `device01`, `gateway01`, `svc-ingest`, `dashboard` (A), `bridge-a`, `viewer` (B) |
| `init-secrets.sh` | `mosquitto_passwd` -> password_file, права 0600 в томе `secrets` |

> Пароли в `users.txt` — только для учебного стенда. В production password_file генерируется при деплое из секретов и в git не хранится.

## Запуск
```
docker compose up -d --build
docker exec lab4-client python run_all.py          # все 8 проверок, ~3 мин
docker exec lab4-client python run_all.py 3 4      # только выбранные
```
EMQX Dashboard: http://localhost:18083 (admin / public). Порты: A — 1883, B — 1884, EMQX — 1885.

## 8 проверок
| № | Скрипт | Что проверяется |
|---|---|---|
| 1 | `check1_auth.py` | анонимы и неверные пароли отклоняются, 4 пользователя входят |
| 2 | `check2_acl.py` | публикация в чужой топик блокируется (MQTT 5: Not authorized), доставка фильтруется по правам |
| 3 | `check3_persistence.py` | persistent-сессия и 50 сообщений в очереди переживают рестарт брокера |
| 4 | `check4_limits.py` | 2 КБ > message_size_limit отклоняется; окно inflight = 20; очередь 100, сверх — dropped |
| 5 | `check5_bridge.py` | out/in направления моста, отсутствие петель на `both`-топике |
| 6 | `check6_sys.py` | метрики `$SYS/#` без нагрузки и под нагрузкой |
| 7 | `check7_emqx.py` | кластер EMQX из 2 узлов, маршрутизация между узлами, REST API |
| 8 | `check8_shared.py` | `$share/g1/...`: 3 consumer'а делят поток ~поровну (EMQX и Mosquitto) |

Результаты: `results/check1..8.json`, `results/summary.md`, `results/sys_metrics.json`, `results/shared.json`.
