import re
from invoke import StreamWatcher

from clients.connection import ConnectionBridge
from clients.utils import secure, timestamp13

from loguru import logger


class KafkaStreamWatcher(StreamWatcher):
    def __init__(self, client: ConnectionBridge, max_timestamp: int):
        self.client = client
        self.max_timestamp = timestamp13(max_timestamp)
        self.pid = None
        self.killed = False

    def submit(self, stream: str):
        if not self.pid:
            match = re.search(r'PID:(\d+)', stream)
            self.pid = match.group(1) if match else None

        if (ts := self.get_latest_timestamp(stream)) > self.max_timestamp and not self.killed:
            logger.debug(f'Max timestamp found {ts} > {self.max_timestamp}. Killing process {self.pid}')
            self.client.run(f"kill -SIGKILL {self.pid}")
            self.killed = True

        return []

    @secure(alt_value=0, silent=True)
    def get_latest_timestamp(self, stream: str) -> int:
        last_line = re.search(r".*\n([^\n]+)[ \n]*$", stream, re.S).group(1)
        return int(re.search(r'CreateTime:(\d*)', last_line).group(1))
