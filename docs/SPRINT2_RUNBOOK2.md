# Sprint 2 — Telegram Receiver / Runbook

## Цель спринта

Receiver должен сутки непрерывно собирать новости из 4 каналов
(@interfaxonline, @rian_ru, @tass_agency, @rbc_news), дедуплицировать
их по `text_hash`, публиковать в `news:raw` и слать heartbeat'ы каждые
30 секунд в `system:heartbeats`.

Целевые цифры из брифа: ~1000 уникальных событий за 24h, 0 дублей.

## Структура проекта

```
D:\quik_sber\newsbot\newsbot3\
├── .env                          # секреты (gitignore)
├── .env.example
├── src\
│   ├── contracts\                # Sprint 1
│   ├── infra\                    # Sprint 1
│   └── services\receiver\        # Sprint 2
│       ├── __init__.py
│       ├── __main__.py
│       ├── client.py
│       ├── config.py
│       ├── event_builder.py
│       └── heartbeat.py
├── scripts\
│   ├── inspect_stream.py
│   └── analyze_soak.py
├── tests\services\receiver\
│   ├── __init__.py
│   └── test_event_builder.py
├── data\sessions\                # Telethon session files (gitignore)
└── docs\
    ├── SPRINT1_DONE.md
    ├── SPRINT2_RUNBOOK.md         # this file
    └── SPRINT2_DONE.md            # filled after 24h soak
```

## Запуск 24-часового soak теста

### Вариант A — простой (терминал)

```cmd
cd D:\quik_sber\newsbot\newsbot3
python -m src.services.receiver
```

Оставить терминал открытым на 24 часа. Не нажимать Ctrl+C. Свернуть.

Минусы: при случайном закрытии окна — потеря работы. При перезагрузке Windows — то же.

### Вариант B — production (NSSM-сервис)

NSSM делает любой EXE/BAT Windows-сервисом с auto-restart при падении.

1. Скачать NSSM: https://nssm.cc/download
2. Создать `start_receiver.bat` в корне проекта:
   ```bat
   @echo off
   cd /d D:\quik_sber\newsbot\newsbot3
   call .venv\Scripts\activate.bat
   python -m src.services.receiver
   ```
3. Установить как сервис:
   ```cmd
   nssm install ReceiverService D:\quik_sber\newsbot\newsbot3\start_receiver.bat
   nssm set ReceiverService AppStdout D:\quik_sber\newsbot\newsbot3\logs\receiver.log
   nssm set ReceiverService AppStderr D:\quik_sber\newsbot\newsbot3\logs\receiver.log
   nssm set ReceiverService Start SERVICE_AUTO_START
   nssm start ReceiverService
   ```
4. Проверить что работает:
   ```cmd
   nssm status ReceiverService
   python scripts\inspect_stream.py --stream system:heartbeats --count 3
   ```

> ⚠️ **Перед NSSM**: первый запуск Telethon должен быть **интерактивным** (введение SMS-кода). Сначала запусти вручную — введи код, дождись `data\sessions\receiver.session`, останови. Только потом ставь как сервис.

## Мониторинг во время soak

В любой момент проверить состояние:

```cmd
REM Сколько в news:raw
python scripts\inspect_stream.py --count 5

REM Heartbeats — последние 5 (должны быть свежие, age < 60s)
python scripts\inspect_stream.py --stream system:heartbeats --count 5

REM Полная сводка за последний час
python scripts\analyze_soak.py --hours 1
```

Что насторожить:
- **`age` последнего heartbeat > 60 сек** — receiver висит или мёртв
- **`errors > 0` в счётчиках** — что-то падает в `_dispatch`, смотри лог
- **`XLEN news:raw` не растёт 10+ минут в активные часы** (10:00-22:00 MSK)
  — либо всё реально молчат, либо connection лост
- **`idem_count < XLEN`** — дубли просочились (баг в дедуп логике)

## После 24 часов

```cmd
REM Полный отчёт
python scripts\analyze_soak.py --hours 24 > docs\soak_report.txt

REM Затем перенести цифры в docs\SPRINT2_DONE.md
```

## Stop conditions

- **Чистая остановка**: `Ctrl+C` в терминале (вариант A) или `nssm stop ReceiverService` (вариант B)
- **Аварийная остановка**: `Ctrl+C` дважды или kill — данные в `news:raw` сохранятся (Memurai persistent), session тоже сохранится
- **Полный сброс данных** (если нужно начать чистый soak):
  ```cmd
  REM В CLI Memurai:
  DEL news:raw
  DEL system:heartbeats
  REM Idempotency keys — все ключи idem:news_text:*
  ```
  ⚠️ Это уничтожит всю собранную историю. Делать только если осознанно.
