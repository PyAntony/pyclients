from typing import ClassVar, Optional

from io import StringIO
from attrs import define
from fabric import Connection
from invoke import Result

from loguru import logger


@define
class ConnectionBridge:
    local_flag: ClassVar[str] = '$LOCAL$'
    # use Fabric Connection for remote
    conn: Optional[Connection] = None
    origin: str = "."
    is_local: bool = True
    buffer: StringIO = StringIO()

    def __attrs_post_init__(self):
        self.conn = self.conn if self.conn else Connection(self.local_flag)
        self.is_local = self.conn.host == self.local_flag

    def run(self, *args, **kwargs) -> Result:
        return (self.conn.local if self.is_local else self.conn.run)(*args, **kwargs)

    def run_with(self, *args, **kwargs) -> Result:
        with_pid = kwargs.pop('pid', True)
        pid_cmd = f"echo PID:$$ && " if with_pid else ''
        cmd = f"{pid_cmd}cd {self.origin} && {args[0]}"
        args = (cmd,) + args[1:]

        logger.debug(f'Running `Connection.run` with: {args}, kwargs={kwargs}')
        return (self.conn.local if self.is_local else self.conn.run)(*args, **kwargs)

    def run_buffered(self, *args, **kwargs) -> Result:
        kwargs['out_stream'] = self.buffer
        return self.run_with(*args, **kwargs)

    def drain_buffer(self) -> str:
        tmp_buffer = self.buffer
        self.buffer = StringIO()
        tmp_buffer.seek(0)
        return tmp_buffer.read()

    def open(self):
        self.is_local or self.conn.open()

    def close(self):
        self.conn.close()

    def connected(self) -> bool:
        return self.is_local or self.conn.is_connected

    def set_origin(self, origin_path: str) -> None:
        self.origin = origin_path

    def extend_holder(self, other: object):
        other.run = self.run
        other.run_with = self.run_with
        other.run_buffered = self.run_buffered
