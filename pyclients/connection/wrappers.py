from typing import ClassVar, Optional, Callable, Union

from toolz import identity
from attrs import define
from invoke import ThreadException
from invoke import Result as InvokeResult
from paramiko.ssh_exception import SSHException, ChannelException
from loguru import logger

from pyclients.abc.types import CMD
from pyclients.utils import fmt_exception, or_else


@define
class RunResult:
    # possible exceptions when running async (multiple sessions per connection will throw exceptions)
    async_errors: ClassVar[tuple[type]] = (ChannelException, SSHException, ThreadException, EOFError)

    cmd: CMD
    result: Union[Exception, InvokeResult]

    def has_async_error(self):
        return isinstance(self.result, self.async_errors)

    def stdout(self) -> str:
        return self.result.stdout if isinstance(self.result, InvokeResult) else ''

    def lines(self, *filter_outs, mapper=identity) -> list[str]:
        filters = (identity,) + filter_outs
        lines = self.stdout().strip().split('\n')

        return [mapper(ln) for ln in lines if all(f(ln) for f in filters)]

    def get_error(self) -> Optional[Exception]:
        return or_else(self.result, isinstance(self.result, Exception))

    def __repr__(self):
        status = fmt_exception(self.result) if self.get_error() else str(self.result.return_code)
        return f"ResultAsyncPair(cmd='{self.cmd}', status={status}, async_error={self.has_async_error()})"


@define
class ResultContainer:
    results: list[RunResult]

    def __attrs_post_init__(self):
        [
            logger.warning(f'CMD `{result.cmd}` raised exception: {result.result}')
            for result in self.results
            if bool(result.get_error())
        ]

    def stdout(self) -> str:
        return '\n'.join(res.stdout() for res in self.results)
