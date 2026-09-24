# src/infra/idempotency.py
from redis.asyncio import Redis


class IdempotencyGuard:
    """SET с TTL — если key уже есть, значит уже обработали."""
    
    def __init__(self, redis: Redis, ttl_seconds: int = 86_400):
        self.redis = redis
        self.ttl = ttl_seconds
    
    async def claim(self, scope: str, key: str) -> bool:
        """
        True — мы первые, можно обрабатывать.
        False — уже обработано (или в работе).
        """
        redis_key = f"idem:{scope}:{key}"
        # SET NX EX — атомарно
        result = await self.redis.set(redis_key, "1", nx=True, ex=self.ttl)
        return result is True