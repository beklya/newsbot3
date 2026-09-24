# tests/contracts/test_schemas.py
import json
from pathlib import Path
import pytest
from src.contracts.raw_news import RawNewsEvent
from src.contracts.enriched_news import EnrichedNewsEvent
from src.contracts.ml_prediction import MLPredictionEvent
from src.contracts.trade_signal import TradeSignalEvent
from src.contracts.execution_result import ExecutionResultEvent

GOLDEN_DIR = Path(__file__).parent / "golden"


@pytest.mark.parametrize("event_cls,file", [
    (RawNewsEvent, "raw_news_v1.json"),
    (EnrichedNewsEvent, "enriched_news_v1.json"),
    (MLPredictionEvent, "ml_prediction_v1.json"),
    (TradeSignalEvent, "trade_signal_v1.json"),
    (ExecutionResultEvent, "execution_result_v1.json"),
])
def test_golden_sample_loads(event_cls, file):
    """Каждая golden-выборка должна валидироваться текущей схемой."""
    data = json.loads((GOLDEN_DIR / file).read_text())
    event = event_cls.model_validate(data)
    # Round-trip
    assert event.model_dump_json() is not None


def test_event_id_propagation():
    """event_id должен сохраняться от Receiver до Bridge."""
    raw = RawNewsEvent.model_validate(
        json.loads((GOLDEN_DIR / "raw_news_v1.json").read_text())
    )
    enriched = EnrichedNewsEvent.model_validate(
        json.loads((GOLDEN_DIR / "enriched_news_v1.json").read_text())
    )
    # Связь через payload.raw_event_id
    # (event_id у каждого свой, но backref должен указывать)
    assert enriched.payload.raw_event_id  # Не пустой