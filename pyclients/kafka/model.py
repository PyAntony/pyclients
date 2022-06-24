import json
import re
from attrs import define, evolve
from toolz import excepts, apply, compose, second, drop
from typing import Union, Optional, Type, Callable
from functools import partial
from json import JSONDecodeError
from loguru import logger

from pyclients.kafka.commands import KEY_SEP, HEADERS_SEP
from pyclients.utils import head_, tail, regx_plus, secure


@define
class TopicPartitionOffset:
    topic: str
    partition: int
    offset: int

    @classmethod
    @secure()
    def from_string(cls: type, raw: str) -> 'TopicPartitionOffset':
        triple = raw.strip().split(':')
        logger.trace(f"Found TopicPartitionOffset: {triple}")

        return cls(
            head_(triple),
            int(second(triple)),
            int(head_(drop(2, triple)))
        )

    @classmethod
    def from_lines(cls: Type['TopicPartitionOffset'], raw: str) -> list['TopicPartitionOffset']:
        return [
            cls.from_string(token)
            for token in raw.strip().split('\n')
            if re.match(r'^[\da-z-_]+:\d+:\d+$', token, re.I)
        ]


@define
class ConsumerRecord:
    timestamp: int
    partition: int
    offset: int
    key: Optional[str]
    value: Union[dict, str]
    headers: list[tuple[str]]

    @classmethod
    @secure(silent=False)
    def from_string(cls: type, raw: str):
        headers, key, payload = raw.split(KEY_SEP)[-3:]
        fmt_key = lambda s: s if s.lower() not in ['null'] else None
        fmt_payload = lambda s: excepts(JSONDecodeError, lambda st: json.loads(st), lambda e: s)(s)
        fmt_headers = lambda s: [tuple(pair.split(':')) for pair in s.split(HEADERS_SEP) if ':' in pair]

        return cls(
            regx_plus(raw, r'CreateTime:(\d+)', 1, int),
            regx_plus(raw, r'Partition:(\d+)', 1, int),
            regx_plus(raw, r'Offset:(\d+)', 1, int),
            fmt_key(key),
            fmt_payload(payload),
            fmt_headers(headers)
        )

    @classmethod
    def from_lines(cls: Type['ConsumerRecord'], raw: str) -> list['ConsumerRecord']:
        return [
            cls.from_string(token)
            for token in raw.strip().split('\n')
            if KEY_SEP in token
        ]


@define
class ProducerRecord:
    key: Optional[str]
    payload: Union[dict, str]

    def to_raw(self):
        is_dict = isinstance(self.payload, dict)
        _payload = json.dumps(self.payload) if is_dict else self.payload

        return _payload if not self.key else f'{self.key}{KEY_SEP}{_payload}'


@define
class PartitionReplicaConfig:
    partition: int
    replica_factor: int
    config: str

    def comb(self) -> str:
        return f"partitions={self.partition},replication={self.replica_factor},{self.config}"

    @classmethod
    @secure()
    def from_string(cls: type, raw: str):
        return cls(
            regx_plus(raw, r'PartitionCount:\s?(\d+)', 1, int),
            regx_plus(raw, r'ReplicationFactor:\s?(\d+)', 1, int),
            regx_plus(raw, r'Configs:\s?(.*?)\n', 1),
        )
