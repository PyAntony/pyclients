from functools import partial
from typing import Type, Union, Callable

from pyclients.cmd.command import CmdType


def cmd_builder(cmd: Type[CmdType], acc=None, **kwargs) -> Union[str, Callable]:
    acc = {} if not acc else acc
    return cmd(**acc).get() if not kwargs else partial(cmd_builder, cmd=cmd, acc={**acc, **kwargs})
