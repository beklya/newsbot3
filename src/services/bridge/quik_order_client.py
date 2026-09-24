"""QuikOrderClient — file-based транспорт ордеров Python ↔ QUIK Lua.

Симметрично quik_live/candle_dump.lua (свечи QUIK→Python через CSV append):
  - Python пишет заявки в orders.jsonl (append, по одной JSON-строке).
  - order_bridge.lua читает новые строки по trans_id, шлёт sendTransaction,
    пишет callbacks (TRANS_REPLY / ORDER / TRADE) в status.jsonl.
  - QuikOrderClient.poll_status() читает новые строки status.jsonl (offset-трекинг,
    как readers.py в quik_feed) и отдаёт их потребителю (RealExecutor).

Идемпотентность: trans_id детерминирован из event_id через persisted transmap
(event_id → trans_id). Повторная доставка сигнала переиспользует trans_id —
order_bridge.lua дедуплицирует по нему, второй заявки не возникает.

Транспорт намеренно file-based, а не сокет: проще, надёжнее на обрыве,
переживает рестарт любой стороны, и уже доказан candle_dump.lua в этой среде.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Literal, Optional

log = logging.getLogger(__name__)

# ENTRY      — рыночная/marketable-limit заявка входа
# EXIT_OCO   — комбинированная QUIK-стоп-заявка «Тэйк-профит и стоп-лимит»
#              (server-side OCO: одна нога отменяет другую на стороне QUIK,
#               без race; переживает обрыв Python)
# CLOSE_MKT  — принудительное рыночное закрытие (time-stop / kill)
# CANCEL     — снять заявку по order_num
OrderKind = Literal["ENTRY", "EXIT_OCO", "CLOSE_MKT", "CANCEL"]


@dataclass(frozen=True)
class OrderRequest:
    """Одна строка orders.jsonl — заявка для order_bridge.lua."""
    trans_id: int
    kind: OrderKind
    account: str
    class_code: str
    sec_code: str
    operation: Literal["B", "S"]      # buy / sell (для EXIT — сторона закрытия)
    order_type: Literal["M", "L"]     # market / limit
    quantity: int
    price: float = 0.0                # лимит-цена входа (0 для market)
    stop_price: float = 0.0           # SL-триггер (EXIT_OCO)
    tp_price: float = 0.0             # TP-цена (EXIT_OCO)
    cancel_order_num: int = 0         # для CANCEL
    client_code: str = ""
    ts: str = ""


@dataclass(frozen=True)
class StatusEvent:
    """Одна строка status.jsonl — callback из QUIK."""
    kind: str                  # TRANS_REPLY | ORDER | TRADE | ERROR
    trans_id: int
    order_num: int = 0
    status: str = ""           # текст статуса/ошибки
    quantity: int = 0
    price: float = 0.0
    commission: float = 0.0    # реальная комиссия (брокер+биржа) из OnTrade
    ts: str = ""


class QuikOrderClient:
    """Append-only writer заявок + offset-tracking reader статусов."""

    def __init__(self, orders_path: Path, status_path: Path,
                 transmap_path: Path) -> None:
        self.orders_path = Path(orders_path)
        self.status_path = Path(status_path)
        self.transmap_path = Path(transmap_path)
        self._lock = threading.Lock()
        self._status_offset = 0
        self._transmap: dict[str, int] = {}
        self._next_trans = 1
        self._load_transmap()
        self.orders_path.parent.mkdir(parents=True, exist_ok=True)
        # начинаем читать статусы с конца уже существующего файла (не реагируем
        # на исторические callbacks прошлой сессии — те уже обработаны/восстановлены
        # через reconcile из QUIK позиций)
        if self.status_path.exists():
            self._status_offset = self.status_path.stat().st_size

    # --- transmap (идемпотентность) ---
    def _load_transmap(self) -> None:
        if self.transmap_path.exists():
            try:
                self._transmap = json.loads(self.transmap_path.read_text("utf-8"))
                if self._transmap:
                    self._next_trans = max(self._transmap.values()) + 1
            except Exception as e:
                log.warning("transmap load failed: %s", e)

    def _save_transmap(self) -> None:
        tmp = self.transmap_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._transmap), encoding="utf-8")
        tmp.replace(self.transmap_path)  # атомарная замена

    def trans_id_for(self, event_id: str) -> tuple[int, bool]:
        """Вернёт (trans_id, is_new). is_new=False → сигнал уже отправлялся."""
        with self._lock:
            if event_id in self._transmap:
                return self._transmap[event_id], False
            tid = self._next_trans
            self._next_trans += 1
            self._transmap[event_id] = tid
            self._save_transmap()
            return tid, True

    # --- write order ---
    def send(self, req: OrderRequest) -> None:
        line = json.dumps(asdict(req), ensure_ascii=False)
        with self._lock:
            with self.orders_path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
        log.info("order_sent trans_id=%s kind=%s %s/%s %s %s qty=%d @ %.4f",
                 req.trans_id, req.kind, req.class_code, req.sec_code,
                 req.operation, req.order_type, req.quantity, req.price)

    # --- read statuses ---
    def poll_status(self) -> list[StatusEvent]:
        if not self.status_path.exists():
            return []
        out: list[StatusEvent] = []
        with self._lock:
            with self.status_path.open("r", encoding="utf-8") as f:
                f.seek(self._status_offset)
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                        out.append(StatusEvent(
                            kind=d.get("kind", ""),
                            trans_id=int(d.get("trans_id", 0)),
                            order_num=int(d.get("order_num", 0)),
                            status=str(d.get("status", "")),
                            quantity=int(d.get("quantity", 0)),
                            price=float(d.get("price", 0.0)),
                            commission=float(d.get("commission", 0.0)),
                            ts=str(d.get("ts", "")),
                        ))
                    except Exception as e:
                        log.warning("bad status line: %s (%s)", line[:120], e)
                self._status_offset = f.tell()
        return out
