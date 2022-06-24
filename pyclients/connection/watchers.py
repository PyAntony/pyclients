"""
Watchers monitor running process stdout in a different thread and react according to conditions:
https://docs.pyinvoke.org/en/stable/api/watchers.html#invoke.watchers.StreamWatcher
"""

import re
from typing import Callable, Any

from invoke import StreamWatcher

from pyclients.abc.types import CMDRunner
from loguru import logger


class LastPrintedLineWatcher(StreamWatcher):
    """
    Stream watcher kills running process when predicate (`line_predicate`) returns true for last line of the
    output stream. Watcher searches for PID in the stream, so it is required for main runner to print the PID
    as first command (e.g., main running command: 'echo PID:$$ && <other commands>').
    """

    def __init__(self, cmd_runner: CMDRunner, line_predicate: Callable[[str], bool]):
        self.cmd_runner = cmd_runner
        self.predicate = line_predicate
        self.pid = None
        self.killed = False

    def submit(self, stream: str):
        if not self.pid:
            match = re.search(r'PID:(\d+)', stream)
            self.pid = match.group(1) if match else None

        if self.pid and not self.killed and self.predicate(last_line := find_last_line(stream)):
            self.cmd_runner(f"kill -SIGKILL {self.pid}")
            self.killed = True
            logger.debug(f"Process '{self.pid}' killed on printed line '{last_line}'")

        return []


def find_last_line(stream: str) -> str:
    """Get last line in `stream`. Regex runs in O(1) independent of `stream` size."""
    return m.group(1) if (m := re.search(r".*\n([^\n]+)[ \n]*$", stream, re.S)) else ''
