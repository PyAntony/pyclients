from collections.abc import Sequence
from functools import reduce
from typing import ClassVar, Optional, Callable, Union

from io import StringIO
from fabric import Connection
from toolz import excepts, groupby, countby
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from loguru import logger

from pyclients.abc.types import CMD
from pyclients.connection.wrappers import RunResult, ResultContainer
from pyclients.connection.watchers import ProcessKiller
from pyclients.utils import inspect_call, join_posix


class ConnectionBridge:
    """
    Wrapper on fabric's Connection. Class provides a single API to run commands independent of
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

    def get_prefix_cmd(self) -> str:
        cdir: str = 'cd ' + join_posix(*self.conn.command_cwds)
        prefix = ' && '.join([cdir] + self.conn.command_prefixes)
        prefix = prefix + ' &&' if prefix else ''
        return prefix.strip()

    @inspect_call(return_val=False, skip_error_on=[lambda e: True])
    def run_basic(self, command: CMD, **kwargs):
        """Fabrik's merged `run` command (local/remote) with some defaults."""
        kwargs['warn'] = kwargs.pop('warn', True)
        kwargs['hide'] = kwargs.pop('hide', True)

        return (self.conn.local if self.is_local else self.conn.run)(command, **kwargs)

    def run_plus(
            self,
            command: CMD,
            force_termination: Callable[[str], bool] = None,
            **kwargs
    ) -> RunResult:
        """Wrapper on Fabrik's runner to apply custom logics, e.g., setup watchers, capture exceptions, etc."""
        buffer = StringIO()
        # buffer not used as it is "apparently" not needed; captured Exception returns seen stdout
        line_watcher = ProcessKiller(self.run_basic, force_termination, buffer=None)
        kwargs['watchers'] = [] if not force_termination else [line_watcher]

        pair = excepts(Exception, lambda cm: (cm, self.run_basic(cm, **kwargs)), lambda e: (command, e))(command)
        return RunResult(pair[0], pair[1], buffer.getvalue())

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
        """
        Run commands with multithreading. Additional parameters are used to adjust threads dynamically; this
        is required since multiple threads open multiple sessions on the same connection and this number
        (sessions limit) is different on each server.

        :param cmds: commands to run.
        :param threads: number of threads to use.
        :param success_rate_continue: if success rate (successful runs) is >= this value then keep running commands
            with the same number of threads, else decrease number of threads on next run.
        :param thread_num_decrease: decrease thread count by this number.
        :param no_progress_max: if not progress has been made in `no_progress_max` iterations then stop iteration
            and return whatever has been accumulated.
        """
        self.reset()
        acc = list()

        while True:
            logger.debug(f"Async running. Total cmds: {len(cmds)}, threads: {threads}")
            with ThreadPoolExecutor(max_workers=threads) as executor:
                curr_results: list[RunResult] = list(executor.map(lambda s: self.run_plus(s, **runner_kwargs), cmds))

            # {True: <pairs-with-errors>, False: <others>}
            res_gp: dict = groupby(lambda pair: bool(pair.get_error()), curr_results)
            counted = countby(lambda pair: bool(pair.get_error()), curr_results)
            failures = counted.get(True, 0)
            successes = counted.get(False, 0)
            all_ = failures + successes
            no_progress = failures == len(cmds)
            success_rate = 1.0 if not all_ else round(successes / all_, 2)
            force_stopped = any(r.force_stopped() for r in curr_results)
            global_succ_rate = 100 * round(len(acc) / (len(acc) + len(cmds)), 2)

            logger.info(
                "Progress - successNow: {} (%{}), accTotalSuccess: {} (%{}), forceTermination: {}",
                successes, 100 * success_rate, len(acc), global_succ_rate, force_stopped
            )

            threads -= (0 if success_rate >= success_rate_continue else thread_num_decrease)
            no_progress_max -= (1 if no_progress else 0)

            if force_stopped or success_rate >= 1 or threads < 1 or no_progress_max < 1:
                acc.extend(curr_results)
                break
            # while some progress, add good responses to acc and set failed commands for rerun
            else:
                acc.extend(res_gp.get(False, []))
                cmds = [pair.cmd for pair in res_gp.get(True, [])]
                self.reset()

        return ResultContainer(acc, force_stopped)

    def run_async2(self, cmds: Union[CMD, Sequence[CMD]], suffix: CMD = '', max_chars=1000, **kwargs) -> list[str]:
        """Extend commands with the '&' character (tasks will run simultaneously) and pass them to next async func."""
        cmds_: list[str] = [cmds] if isinstance(cmds, str) else cmds

        with manual_extension(self, cmds_, suffix) as full_commands:
            merged = merge_by_max_size(full_commands, lambda a, b: f"{a} & {b}", max_chars)
            cmds_ready: list[CMD] = [f"{cmd} & wait" for cmd in merged]
            # cmds_: ['cmd1 & cmd2 & ... & wait', 'cmd1 & cmd2 & ... & wait', ...]
            return self.run_async(cmds_ready, **kwargs).lines()


def merge_by_max_size(strings: list[str], merge_with: Callable, total_max_size: int) -> list[str]:
    """Append strings up total `total_max_size`."""
    acc = list()

    def reducer(partial_acc, str_):
        if len(str_) > total_max_size:
            raise CommandSizeExceed(str_, total_max_size)

        if len(merged := merge_with(partial_acc, str_)) > total_max_size:
            acc.append(partial_acc)
            return str_
        else:
            return merged

    return acc + [reduce(reducer, strings)]


class CommandSizeExceed(Exception):
    def __init__(self, cmd: str, max_size: int):
        self.cmd = cmd
        self.max_size = max_size
        super().__init__(f"Cmd '{cmd}' exceeded max number of characters: '{max_size}'")


@contextmanager
def manual_extension(cb: ConnectionBridge, cmds: list[CMD], suffix: str = ''):
    """
    Add prefix (Invoke's `command_cwds` and `command_prefixes`) and suffix to commands. Empty Invoke's prefixes
    since they have been already appended to commands.
    """
    tmp_cwds = cb.conn.command_cwds
    tmp_prefixes = cb.conn.command_prefixes
    # add prefix and suffix
    full_cmds = [' '.join([cb.get_prefix_cmd(), command, suffix]).strip() for command in cmds]

    cb.conn.command_cwds = []
    cb.conn.command_prefixes = []

    try:
        yield full_cmds
    finally:
        logger.debug("Bridge prefixes: cwds: {}, prefixes: {}", cb.conn.command_cwds, cb.conn.command_prefixes)
        cb.conn.command_cwds = tmp_cwds
        cb.conn.command_prefixes = tmp_prefixes
