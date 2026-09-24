# src/infra/publisher.py
import json
import time
from typing import Type
from redis.asyncio import Redis
from src.contracts.base import MessageEnvelope


class StreamPublisher:
    def __init__(self, redis: Redis, stream: str, maxlen: int = 50_000):
        self.redis = redis
        self.stream = stream
        self.maxlen = maxlen
    
    async def publish(self, event: MessageEnvelope) -> str:
        """Возвращает Redis stream ID."""
        # Добавляем trace перед публикацией
        event = event.add_trace(self.stream)
        
        payload = event.model_dump_json()
        msg_id = await self.redis.xadd(
            self.stream,
            fields={"data": payload, "event_id": event.event_id},
            maxlen=self.maxlen,
            approximate=True,
        )
        return msg_id

