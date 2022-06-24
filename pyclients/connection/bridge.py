from typing import ClassVar, Optional, Callable

from io import StringIO
from fabric import Connection
from toolz import excepts, groupby, countby
from concurrent.futures import ThreadPoolExecutor
from loguru import logger

from pyclients.abc.types import CMD, CMDRunner
from pyclients.connection.wrappers import RunResult, ResultContainer
from pyclients.connection.watchers import LastPrintedLineWatcher
from pyclients.utils import inspect_call


class ConnectionBridge:
    """
    Wrapper around fabric's Connection. Class provides a single API to run commands independent of
    Connection being local or remote. Passing a remote Connection during instantiation is optional (local by default).
    """
    local_flag: ClassVar[str] = '$LOCAL$'
    local_connection: ClassVar[Connection] = Connection('$LOCAL$')

    def __init__(self, conn: Optional[Connection] = None):
        self.conn = conn or self.local_connection
        self.is_local = self.conn.host == self.local_flag
        self.conn.command_prefixes = ['echo PID:$$']
        self.buffer: StringIO = StringIO()

    def open(self):
        self.is_local or self.conn.open()

    def close(self):
        self.conn.close()

    def reset(self):
        self.close()
        self.open()

    def connected(self) -> bool:
        return self.is_local or self.conn.is_connected

    def set_origin(self, dir_: str):
        self.conn.command_cwds = [dir_]

    @inspect_call(return_val=False)
    def run_(self, command: CMD, **kwargs):
        kwargs['hide'] = kwargs.pop('hide', True)

        return (self.conn.local if self.is_local else self.conn.run)(command, **kwargs)

    def run(self, command: CMD, stopper: Callable[[str], bool] = None, **kwargs) -> RunResult:
        kwargs['warn'] = kwargs.pop('warn', True)
        kwargs['hide'] = kwargs.pop('hide', True)
        kwargs['watchers'] = [] if not stopper else [LastPrintedLineWatcher(self.run_, stopper)]

        pair = excepts(Exception, lambda cm: (cm, self.run_(cm, **kwargs)), lambda e: (command, e))(command)
        return RunResult(pair[0], pair[1])

    def run_async(
            self,
            cmds: list[CMD],
            *,
            threads=8,
            success_rate_continue=0.5,
            thread_num_decrease=2,
            no_progress_max=3,
            **runner_kwargs
    ) -> ResultContainer:
        self.reset()
        responses = list()

        while True:
            logger.debug(f"Async running. Total cmds: {len(cmds)}, threads: {threads}")
            with ThreadPoolExecutor(max_workers=threads) as executor:
                results: list[RunResult] = list(executor.map(lambda s: self.run(s, **runner_kwargs), cmds))

            # {True: <pairs-with-async-errors>, False: <others>}
            res_gp: dict = groupby(lambda pair: pair.has_async_error(), results)
            counted = countby(lambda r: r.has_async_error(), results)
            failures = counted.get(True, 0)
            successes = counted.get(False, 0)
            all_ = failures + successes
            no_progress = failures == len(cmds)
            no_async_failures = failures == 0
            success_rate = 1.0 if not all_ else round(successes / all_, 2)

            logger.debug(f"Progress - successes: {successes}(%{100 * success_rate}), resultsAgg: {len(responses)}")
            threads -= (0 if success_rate > success_rate_continue else thread_num_decrease)
            no_progress_max -= (1 if no_progress else 0)
            responses.extend(res_gp.get(False, []))

            if no_async_failures or threads < 1 or no_progress_max < 1:
                responses.extend(res_gp.get(True, []))
                break
            # while some progress
            else:
                cmds = [pair.cmd for pair in res_gp.get(True, [])]
                self.reset()

        return ResultContainer(responses)
