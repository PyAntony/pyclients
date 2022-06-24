"""
Utility functions.
"""
import operator
import re
import threading
import functools
import traceback
import isodate
from datetime import datetime
from toolz import excepts, first, identity, apply, last

from loguru import logger
from typing import Callable, Any

from pyclients.abc.types import Number


def set_attr(obj, name, value):
    setattr(obj, name, value)
    return obj


def or_else(val, condition, alt=None):
    return val if condition else alt


def head_(sequence):
    return excepts((StopIteration, IndexError), lambda seq: first(seq), lambda e: None)(sequence)


def last_(sequence):
    return excepts((StopIteration, IndexError), lambda seq: last(seq), lambda e: None)(sequence)


def tail(sequence):
    return sequence[1:]


def regx_plus(string: str, regx: str, gp: int = 0, converter: Callable = identity) -> Any:
    match = re.search(regx, string)
    return apply(converter, re.search(regx, string).group(gp)) if match else None


def timestamp13(ts: Number) -> int:
    """Converts a 10 digit (with/without decimals) timestamp to 13 digits."""
    num = int(str(float(ts) * 1000)[:13])

    if len(str(num)) == 13:
        return num
    else:
        raise Exception(f"Unable to produce 13 digit number from {ts}")


def fmt_exception(e: Exception):
    return f"{type(e).__name__}{e.args}"


def now_with_delta(chars: str, func=operator.sub):
    if not re.match(r'\d+[dhms]', chars, re.I):
        raise Exception(f"Delta expression '{chars}' doesn't match pattern: '\d+[dhms]'")

    chars_ = chars.upper()
    delta_exp = re.sub(r'\d+' + last(chars_), chars_, 'P0DT0H0M0S')
    delta = isodate.parse_duration(delta_exp)

    return func(datetime.now(), delta).timestamp()


def do_when(cond: bool, val: Any, func: Callable) -> Any:
    """Call func on val if condition is true (only the side effects of 'func' are relevant)."""
    cond and func(val)
    return val


def secure(*exceptions, alt_value=None, silent=False):
    """
    Decorator returns `return_` value if function raises an exception (in matched `exceptions`),
    else return computed value. If no exceptions is passed then it catches all exceptions.
    """

    def decorator(func):
        def error_handler(e: Exception):
            if not silent:
                logger.error(f'Thread:{threading.get_ident()} Func:{func.__name__} - {e.__class__}: {str(e)}')
            return alt_value

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            errors = (Exception,) if not exceptions else exceptions
            return excepts(errors, lambda: func(*args, **kwargs), error_handler)()

        return wrapper

    return decorator


def inspect_call(arguments=True, return_val=True, self=False):
    """
    Decorator logs exception traceback (if any), arguments, and return value.
    """

    def decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            args_ = args[1:] if not self else args
            do_when(arguments, f'Callable({func.__name__}). Args: {args_}. Kwargs: {kwargs}.', logger.debug)

            try:
                return_ = func(*args, **kwargs)
                do_when(return_val, f'Callable({func.__name__}) returns: {return_}', logger.debug)

                return return_

            except Exception as e:
                logger.warning(traceback.format_exc())
                raise e

        return inner

    return decorator
