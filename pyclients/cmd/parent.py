import re

from attr._make import Attribute
from attrs import evolve
from typing import Optional, cast

from pyclients.utils import or_else, set_attr


class CmdType:
    """Class to be extended by command classes."""

    def __init__(self, *args, **kwargs):
        ...

    def get(self) -> str:
        tmp_obj = evolve(self)
        [
            setattr(tmp_obj, att.name, cmd_mapper(tmp_obj, getattr(tmp_obj, att.name)))
            for att in tmp_obj.__attrs_attrs__
            if (cmd_mapper := cast(Attribute, att).metadata.get('F'))
        ]

        return repr(tmp_obj)

    def when(self, cond: bool) -> Optional['CmdType']:
        """Returns self or None if condition is False."""
        return or_else(self, cond)

    def __repr__(self):
        """
        Display all fields of this class sequentially. Only values are displayed, i.e.,
        field names are ignored.
        """
        # example: [('flag', '--topic'), ('value', 'myTopic')]
        pairs = self.__dict__.items()
        command = " ".join(repr(val) for (k, val) in pairs if val is not None)
        # clean final string
        command = re.sub(r"\s{2,}", " ", command).replace("'", "").strip()

        return command
