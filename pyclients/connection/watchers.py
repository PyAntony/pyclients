"""
Watchers monitor running process stdout in a different thread and react according to conditions:
https://docs.pyinvoke.org/en/stable/api/watchers.html#invoke.watchers.StreamWatcher
"""

import re
from typing import Callable
from io import StringIO
from invoke import StreamWatcher
from loguru import logger

from pyclients.abc.types import CMDRunner


class ProcessKiller(StreamWatcher):
    """
    Stream watcher kills running process when predicate (`line_predicate`) returns true.
    Search can be performed in last line or entire stream. Watcher searches for PID in the stream,
    so it is required (unless raise_exc=True) for main runner to print the PID as first command
    (e.g., main running command: 'echo PID:$$ && <other commands>').
    """

    def __init__(
            self,
            cmd_runner: CMDRunner,
            kill_predicate: Callable[[str], bool],
            buffer: StringIO = None,
            raise_exc: bool = True,
            full_search: bool = True
    ):
        self.cmd_runner = cmd_runner
        self.kill_predicate = kill_predicate
        self.buffer = buffer
        self.raise_exc = raise_exc
        self.full_search = full_search
        self._curr_idx = 0
        self._killed = False
        self._pid = None

    def submit(self, stream: str):
        if not self._pid:
            match = re.search(r'PID:(\d+)', stream[:500], re.I)
            self._pid = match.group(1) if match else None

        # process is killed by PID or raising exception (or both)
        if not self._pid and not self.raise_exc:
            return []

        if not self.full_search:
            stream_section = find_last_line(stream)
        else:
            stream_section = stream[self._curr_idx:]
            new_index = self._curr_idx + len(stream_section)
            logger.trace('PID:{} - Full search idx from "{}", to "{}"', self._pid, self._curr_idx, new_index)
            self._curr_idx = new_index

        if not self._killed and self.kill_predicate(stream_section):
            self.kill_process(stream, stream_section)

        return []

    def kill_process(self, stream: str, section: str):
        if self.buffer:
            self.buffer.seek(0)
            self.buffer.write(stream)

        self.cmd_runner(f"kill -SIGKILL {self._pid}")
        self._killed = True

        logger.info(f"ProcessKiller(PID:{self._pid}) ended process! Exception raised: {self.raise_exc}")
        if self.raise_exc:
            raise ProcessKilledException(f'StreamWatcher Killed running process: {self._pid}')


def find_last_line(stream: str) -> str:
    """Get last line in `stream`. Regex runs in O(1) independent of `stream` size."""
    return m.group(1) if (m := re.search(r".*\n([^\n]+)[ \n]*$", stream, re.S)) else ''


class ProcessKilledException(Exception):
    pass
