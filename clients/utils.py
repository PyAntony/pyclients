import re
import threading
import functools
import traceback
from toolz import excepts, first, identity, apply, last

from loguru import logger
from typing import Callable, Any

from clients.interface.types import Number


def or_else(val, condition, alt=None):
    return val if condition else alt


def to_int(string: str):
    return int(string)


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
    return int(str(float(ts) * 1000)[:13])


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


def do_conditional(cond: bool, val: Any, func: Callable) -> Any:
    """Call func on val if condition is true (only the side effects of 'func' are relevant)."""
    cond and func(val)
    return val


def inspect_func(arguments=True, return_val=True):
    """
    Decorator logs exception traceback (if any), arguments, and return value.
    """

    def decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            logger.debug(f'Inspecting function: *{func.__name__}* ...')
            do_conditional(arguments, f'Args: {args}. Kwargs: {kwargs}.', logger.debug)

            try:
                return_ = func(*args, **kwargs)
                do_conditional(return_val, f'Returning: *{return_}*', logger.debug)

                return return_

            except Exception as e:
                logger.warning(traceback.format_exc())
                raise e

        return inner

    return decorator
