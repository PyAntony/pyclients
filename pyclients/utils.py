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
from pathlib import PurePosixPath
from toolz import excepts, first, identity, apply, last, dissoc
from functools import reduce

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


def dig_in(keys, coll, default=None, raise_=False) -> Any:
    dig = lambda obj, k: getattr(obj, k.replace('@', '', 1)) if '@' in str(k) else obj[k]

    try:
        return reduce(dig, keys, coll)
    except (KeyError, IndexError, TypeError, AttributeError):
        if raise_:
            raise
        return default


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


def fmt_timestamp(timestamp: Number, fmt: str = "%m/%d, %H:%M:%S"):
    dt_object = datetime.fromtimestamp(int(str(timestamp)[:10]))
    return str(dt_object.strftime(fmt))


def fmt_exception(e: Exception):
    return f"{type(e).__name__}: {str(e)}"


def now_with_delta(chars: str, func=operator.sub) -> float:
    if not re.match(r'\d+[dhms]', chars, re.I):
        raise Exception(f"Delta expression '{chars}' doesn't match pattern: '\d+[dhms]'")

    chars_ = chars.upper()
    delta_exp = re.sub(r'\d+' + last(chars_), chars_, 'P0DT0H0M0S')
    delta = isodate.parse_duration(delta_exp)

    return func(datetime.now(), delta).timestamp()


def join_posix(*args):
    return str(PurePosixPath(*args))


def do_when(cond: bool, val: Any, func: Callable) -> Any:
    """Call func on val if condition is true (only the side effects of 'func' are relevant)."""
    cond and func(val)
    return val


def secure(*exceptions, alt_value=None, silent=False):
    """
    Decorator returns `alt_value` value if function raises an exception in matched `exceptions`,
    else return computed value. If no `exceptions` is passed then it catches all exceptions.
    """

    def decorator(func):
        def error_handler(e: Exception, args, kwargs):
            if not silent:
                logger.debug(
                    'Secure caught call({})[thread:{}] - {}, Params: {}, {}',
                    func.__name__, threading.get_ident(), fmt_exception(e), args, kwargs
                )
            return alt_value

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            errors = (Exception,) if not exceptions else exceptions
            return excepts(errors, lambda: func(*args, **kwargs), lambda e: error_handler(e, args, kwargs))()

        return wrapper

    return decorator


def inspect_call(
        arguments=True,
        return_val=True,
        skip_args: tuple = (0,),
        skip_kwargs: tuple = None,
        skip_error_on: list[Callable] = None
):
    """
    Decorator logs exception traceback (if any), arguments, and return value.
    """

    def decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            args_ = tuple(arg[1] for arg in enumerate(args) if arg[0] not in (skip_args or []))
            kwargs_ = dissoc(kwargs, *(skip_kwargs or []))

            try:
                return_ = func(*args, **kwargs)

                if not arguments and not return_val:
                    return return_

                msg = f"Inspect({func.__name__})"
                params = f" - Params: {args_}, {kwargs_}" if arguments else ''
                ret = f' - Returns: {return_}' if return_val else ''
                msg_ = msg + params + ret
                logger.debug(msg_)

                return return_

            except Exception as e:
                log_exception = not any([f(e) for f in (skip_error_on or [])])

                do_when(arguments, f'Inspect({func.__name__}). {args_}, {kwargs_}.', logger.debug)
                do_when(log_exception, traceback.format_exc(), logger.warning)

                raise e

        return inner

    return decorator
