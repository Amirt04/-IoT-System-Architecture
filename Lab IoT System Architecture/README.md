# Программирование IoT систем — лабораторные работы

Магистратура «IoT systems», ЕНУ им. Л.Н. Гумилева.
Автор: Мусин Амир Рустамович, группа M094–6121–26–02.
Преподаватель: Эгамбердиев Э.У.

Все стенды запускаются в Docker (Docker Desktop + Docker Compose), аппаратура не требуется.

| № | Тема | Папка | Запуск |
|---|------|-------|--------|
| 0 | Развёртывание стенда курса: Mosquitto + IoT-симулятор | [lab0](lab0) | `docker compose up -d --build` |
| 1 | Device Layer — датчики и модель данных: JSON Schema, JSON/CBOR, кольцевой буфер | [lab1](lab1) | `docker compose up -d --build` |
| 2 | Wi-Fi Communication — характеристики канала: tc netem, iperf3, ping, MQTT QoS 0/1 | [lab2](lab2) | `docker compose up -d --build`, затем `docker exec lab2-client python measure.py` |

## Lab 0
- `docker-compose.yml` — брокер `mosquitto` и `iot-simulator`
- `mosquitto/config/mosquitto.conf` — listener, persistence, log
- `simulator/simulator.py` — публикация температуры раз в 5 с, Last Will, переподключение
- `tools/measure.py` — замер интервала и размера сообщений

## Lab 1
- `device/model.py` — модели датчиков (суточный цикл, шум, дрейф, выбросы) и валидация
- `device/device.py` — 4 канала, JSON/CBOR, кольцевой буфер на 500 записей
- `schema/telemetry.schema.json` — JSON Schema сообщения
- `collector/collector.py` — проверка схемы и непрерывности `seq`, `ingest_ts`
- `tools/size_compare.py` — таблица «формат → байт/сообщение → трафик/сутки»

## Lab 2
- `client/profiles.py` — 5 профилей канала (RSSI −40…−84 dBm, потери 0–20 %)
- `client/measure.py` — iperf3, ping, MQTT QoS 0/1, время подключения TCP/TLS/MQTT
- `client/analyze.py` — доверительные интервалы, p50/p95/p99, NFR, графики
- `results/` — сырые данные 3 серий, таблицы и графики
