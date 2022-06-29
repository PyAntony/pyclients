from loguru import logger
import sys
import os

# 'LOGURU_LEVEL' env to be set by clients
global_level = os.getenv("LOGURU_LEVEL")


def _level(lev: str, global_=global_level):
    return global_ or lev


handlers = [
    # dict(sink=sys.stdout, filter=lambda r: r["level"].no < 30, level=_level("TRACE")),
    dict(sink=sys.stderr, level=_level("INFO"))
]

logger.remove()
logger.configure(handlers=handlers)

# clients must set env level or enable logger manually
if not global_level:
    logger.disable('pyclients')
