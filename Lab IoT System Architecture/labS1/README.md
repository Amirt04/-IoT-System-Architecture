# Lab S1 — LPWAN: симуляция LoRaWAN и энергопрофилирование

SW-вариант (только ПК): симулятор LoRaWAN на Python вместо ChirpStack и реального шлюза.

```
[N виртуальных end-devices] -> [коллизионная модель ALOHA] -> [gateway model]
        |                                                          |
[энергетическая модель устройства]                     [Network Server: MQTT-мост]
        |                                                          |
[battery lifetime, графики]                             [MQTT -> стенд курса]
```

| Файл | Назначение |
|---|---|
| `sim/airtime.py` | Time on Air по формуле Semtech (аналог `lorawan-airtime`), самопроверка 41,22 мс |
| `sim/network.py` | Модель сети: случайный момент передачи, 3 канала EU868, ортогональность SF, capture effect 6 дБ, шлюз с 8 демодуляторами |
| `sim/energy.py` | Энергомодель класса A: TX, RX1, RX2 (SF12), сон; срок службы батареи |
| `sim/run_all.py` | Все эксперименты и графики -> `results/` |
| `sim/bridge.py` | Воспроизведение сети в реальном времени -> MQTT (uplink в формате ChirpStack v4) |

## Запуск

Вариант 1 — Python на ПК:
```
pip install numpy matplotlib paho-mqtt
cd sim
python airtime.py      # самопроверка формулы ToA
python run_all.py      # ~30 с, результаты в ../results
```

Вариант 2 — Docker:
```
docker compose run --rm --build sim          # симуляция -> ./results
docker compose up -d --build mosquitto bridge   # поток uplink-сообщений на localhost:1883
docker logs -f labs1-bridge
```
Смотреть сообщения: MQTT Explorer -> localhost:1883, топики `application/labs1/...` и `labs1/stats`.

## Результаты (`results/`)
- `fig1_aloha_validation.png` — проверка на пределе ALOHA (18,4 %)
- `fig2_pdr_n_sf.png`, `fig3_pdr_periods.png` — PDR(N) для SF7/SF9/SF12
- `fig4_energy_per_message.png`, `fig5_battery_lifetime.png` — энергобюджет и срок службы
- `fig6_adr_sf12.png` — сценарий «ADR перевёл много устройств на SF12»
- `pdr_grid.csv`, `max_devices.csv`, `battery_lifetime.csv`, `adr_scenario.csv`, `aloha_validation.csv`
