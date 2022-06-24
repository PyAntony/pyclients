import re
import attr
from toolz import identity
from attr._make import Attribute, _CountingAttr, NOTHING
from attrs import define, field, make_class, evolve
from typing import ClassVar, Callable, Union, Optional, Any, cast, Type

from pyclients.utils import or_else, set_attr
from pyclients.cmd.parent import CmdType


def make_flag(
        flag_name: str,
        flag: str,
        repeat: list = None,
        converter: Callable = None
) -> type:
    """
    Creates a class type dynamically. Class represents a string command (flag) with a key-value pair.
    For example, the 'topic' flag from kafka consumer API (kafka-console-consumer.sh):
    --topic myTopic -> TopicToken(flag='--topic', value='myTopic')

    :param flag_name: class name for this flag.
    :param flag: the string flag to be used as the default `flag` value.
    :param repeat: repeats the string flag for each value in this list. For example,
        given `flag='--property'` and `repeat=['print.key=true', 'print.headers=true']`, the final `flag` will be:
        `--property print.key=true --property print.headers=true`

    :param converter: function to add any additional transformation to the value.

    :return: a class that extends `CmdType` type.
    """
    flag = ' '.join(f'{flag} {val}' for val in repeat) if repeat else flag

    return make_class(
        flag_name, {
            "flag": field(init=False, default=flag),
            "value": field(default=None, converter=converter or identity),
        },
        bases=(CmdType, object),
        slots=False,
        repr=False
    )


def token(
        cmd_mapper: Callable[[CmdType, Any], Optional[CmdType]],
        *,
        boolean_flag: bool = False,
        base=NOTHING,
        **kwargs
):
    if boolean_flag:
        kwargs['default'] = None
        kwargs['init'] = False

    if not isinstance(base, NOTHING.__class__):
        kwargs['default'] = base

    return attr.ib(**{'metadata': {'F': cmd_mapper}, **kwargs})


def cmd(main_cmd: str) -> Callable[[type], Type[CmdType]]:
    def builder(cls: type) -> Type[CmdType]:
        types = cls.__annotations__
        others = dict(
            (key, set_attr(c_attr, 'type', types.get(key) or c_attr.type))
            for key in cls.__dict__
            if isinstance(c_attr := getattr(cls, key), _CountingAttr)
        )

        return cast(Type[CmdType], make_class(
            cls.__name__,
            {'bin': attr.ib(init=False, type=str, default=main_cmd), **others},
            bases=(CmdType, object),
            slots=False,
            repr=False
        ))

    return builder
