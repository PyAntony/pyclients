import json
import re
from attrs import define, evolve
from toolz import excepts, apply, compose, second, drop
from typing import Union, Optional, Type, Callable
from functools import partial
from json import JSONDecodeError
from loguru import logger

from clients.kafka.commands import KEY_SEP, HEADERS_SEP
from clients.utils import head_, tail, regx_plus, secure, to_int


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
            to_int(second(triple)),
            to_int(head_(drop(2, triple)))
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
    @secure(silent=True)
    def from_string(cls: type, raw: str):
        regx_convert = partial(regx_plus, raw)
        not_key_sep = f"[^{KEY_SEP}]*"
        headers_reg1 = f'{KEY_SEP}({not_key_sep}{HEADERS_SEP}{not_key_sep}){KEY_SEP}'
        headers_reg2 = f'.*{KEY_SEP}({not_key_sep}){KEY_SEP}{not_key_sep}{KEY_SEP}{not_key_sep}$'
        key_value_reg = f"({not_key_sep}{KEY_SEP}{not_key_sep}$)"

        headers: str = regx_convert(headers_reg1, 1) or regx_convert(headers_reg2, 1) or ''
        split_headers = lambda s: [tuple(pair.split(':')) for pair in s.split(HEADERS_SEP) if ':' in pair]

        key_value: str = regx_convert(key_value_reg, 1)
        get_key = lambda s: head_(s.split(KEY_SEP))
        format_key = lambda s: s if s.lower() not in ['null'] else None
        get_payload = lambda s: head_(tail(s.split(KEY_SEP)))
        format_payload = lambda s: excepts(JSONDecodeError, lambda st: json.loads(st), lambda e: s)(s)

        logger.trace(f"KafkaRecord from str - Headers: {headers}, KeyValue: {key_value}")
        return cls(
            regx_convert(r'CreateTime:(\d+)', 1, to_int),
            regx_convert(r'Partition:(\d+)', 1, to_int),
            regx_convert(r'Offset:(\d+)', 1, to_int),
            apply(compose(format_key, get_key), key_value),
            apply(compose(format_payload, get_payload), key_value),
            apply(split_headers, headers)
        )

    @classmethod
    def from_lines(cls: Type['ConsumerRecord'], raw: str) -> list['ConsumerRecord']:
        return [
            cls.from_string(token)
            for token in raw.strip().split('\n')
            if KEY_SEP in token
        ]

    def map_payload(self, mapper: Callable):
        try:
            return evolve(self, value=mapper(self.value))
        except Exception as e:
            logger.warning(f'mapper failed for value: {self.value} - {e.__class__} {e}')
            return self


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

    @classmethod
    @secure()
    def from_string(cls: type, raw: str):
        return cls(
            regx_plus(raw, r'PartitionCount:\s?(\d+)', 1, to_int),
            regx_plus(raw, r'ReplicationFactor:\s?(\d+)', 1, to_int),
            regx_plus(raw, r'Configs:\s?(.*?)\n', 1),
        )
