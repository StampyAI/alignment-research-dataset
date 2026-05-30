from unittest.mock import patch

import pytest

from align_data.common.retry import with_retry


def test_returns_on_success_without_retrying():
    calls = {"n": 0}

    @with_retry()
    def f():
        calls["n"] += 1
        return "ok"

    assert f() == "ok"
    assert calls["n"] == 1


@patch("align_data.common.retry.time.sleep")
def test_retries_then_succeeds(mock_sleep):
    calls = {"n": 0}

    @with_retry(times=3, exceptions=ValueError)
    def f():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("blip")
        return "ok"

    assert f() == "ok"
    assert calls["n"] == 3
    # backoff before retries 2 and 3 only -- no sleep after the final attempt.
    assert mock_sleep.call_count == 2


@patch("align_data.common.retry.time.sleep")
def test_raises_the_last_original_error_on_exhaustion(mock_sleep):
    """Unlike the old decorators (which raised a generic 'gave up' ValueError),
    the real cause must survive so callers/logs see what actually failed."""

    @with_retry(times=3, exceptions=KeyError)
    def f():
        raise KeyError("the real cause")

    with pytest.raises(KeyError, match="the real cause"):
        f()
    # 3 attempts -> 2 sleeps (none after the last).
    assert mock_sleep.call_count == 2


def test_does_not_retry_unlisted_exceptions():
    calls = {"n": 0}

    @with_retry(times=3, exceptions=ValueError)
    def f():
        calls["n"] += 1
        raise TypeError("not retryable")

    with pytest.raises(TypeError):
        f()
    assert calls["n"] == 1


def test_preserves_function_metadata():
    @with_retry()
    def my_func():
        """docstring."""

    assert my_func.__name__ == "my_func"
    assert my_func.__doc__ == "docstring."
