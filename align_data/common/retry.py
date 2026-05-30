import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)


def with_retry(times=3, exceptions=Exception):
    """Retry the wrapped callable on `exceptions`, with exponential backoff.

    The project-wide helper for simple, stateless transient retries: HTTP timeouts,
    dropped connections, gspread API blips. Up to `times` attempts; sleeps 2**i
    seconds before retry i (1s, 2s, ...) with no sleep after the final attempt. On
    exhaustion it re-raises the LAST original exception, so the real cause survives.
    Anything not listed in `exceptions` propagates immediately -- don't retry what
    won't self-resolve (a 4xx, a bug).

    NOT for stateful or laddered retries, which stay bespoke by design:
    - update_pinecone's DB loops need session.rollback() between attempts;
    - with_pinecone_retry needs per-exception attempt counts (429 vs ProtocolError).
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            last_error = None
            for i in range(times):
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    logger.warning(f"{f.__name__} failed (attempt {i + 1}/{times}): {e}")
                    if i < times - 1:
                        time.sleep(2 ** i)
            raise last_error
        return wrapper
    return decorator
