from __future__ import annotations

import dramatiq
from dramatiq.brokers.redis import RedisBroker

from app.core.config import get_settings


settings = get_settings()
broker = RedisBroker(url=settings.redis_url)
dramatiq.set_broker(broker)

