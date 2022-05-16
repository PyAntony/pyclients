import re
from toolz import identity
from attrs import define, field, make_class
from typing import ClassVar, Callable, Union, Optional

from clients.utils import or_else


class CmdRepr:
    def when(self, cond: bool) -> Optional['CmdRepr']:
        return or_else(self, cond)

    def __repr__(self):
        annotations = self.__dict__.items()
        cmd = " ".join(repr(val) for (k, val) in annotations if val is not None)
        cmd = re.sub(r"\s{2,}", " ", cmd.replace("'", "")).strip()

        return cmd


def make_flag(flag_name: str, flag: str, repeat: list = None, converter: Callable = None):
    flag = ' '.join(f'{flag} {val}' for val in repeat) if repeat else flag

    return make_class(
        flag_name, {
            "flag": field(init=False, default=flag),
            "value": field(default=None, converter=converter or identity),
        },
        bases=(CmdRepr, object),
        repr=False
    )
