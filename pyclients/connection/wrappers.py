from typing import ClassVar, Optional, Union

from toolz import concat
from attrs import define
from invoke import ThreadException
from invoke import Result as InvokeResult
from paramiko.ssh_exception import SSHException, ChannelException
from loguru import logger

from pyclients.abc.types import CMD
from pyclients.utils import or_else, dig_in


@define
class RunResult:
    """
    Wrapper on Invoke's result. Any exception thrown is captured and custom logic is applied
    according to exception type.
    """
    # possible exceptions when running async (multiple sessions per connection will throw exceptions)
    # DEPRECATED: these exceptions could be thrown for multiple reasons (not just max number of sessions)
    async_errors: ClassVar[tuple[type]] = (ChannelException, SSHException, ThreadException, EOFError)

    cmd: CMD
    result: Union[Exception, InvokeResult]
    buffered_stream: str = ''

    def lines(self) -> list[str]:
        result_stdout = getattr(self.result, 'stdout', '')
        buffered_stdout = self.buffered_stream
        from_killed = dig_in(['@exceptions', 0, '@kwargs', 'kwargs', 'buffer_'], self.result, default=[''])
        from_killed = ''.join(from_killed)

        return (result_stdout or buffered_stdout or from_killed).strip().split('\n')

    def get_error(self) -> Optional[Exception]:
        return or_else(self.result, isinstance(self.result, Exception))

    def force_stopped(self) -> bool:
        return (e := self.get_error()) and 'ProcessKilledException' in str(e)

    def __repr__(self):
        status = ((e := self.get_error()) and e.__class__) or f"InvokeResult:{str(self.result.return_code)}"
        return f"RunResult(cmd='{self.cmd}', status={status}, force_stopped={self.force_stopped()})"


@define
class ResultContainer:
    results: list[RunResult]
    force_stopped: bool = False

    def __attrs_post_init__(self):
        [
            logger.warning(f'CMD `{result.cmd}` raised exception: {error}')
            for result in self.results
            if (error := result.get_error()) and not self.force_stopped
        ]

    def lines(self) -> list[str]:
        return list(concat(r.lines() for r in self.results))
