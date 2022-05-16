import os
from attrs import define, field
from clients.cmd import make_flag, CmdRepr
from clients.utils import timestamp13
from clients.interface.types import Number
from typing import Optional, Callable

KEY_SEP = '@@@'
HEADERS_SEP = '###'
CONSUMER_PROPERTIES = [
    "print.timestamp=true",
    "print.key=true",
    "print.offset=true",
    "print.partition=true",
    "print.headers=true",
    "print.value=true",
    f"key.separator={KEY_SEP}",
    f"headers.separator={HEADERS_SEP}"
]
PRODUCER_PROPERTIES = [
    f"key.separator={KEY_SEP}",
    "parse.key=true"
]

BootstrapServerToken = make_flag('BootstrapServerToken', '--bootstrap-server')
TopicToken = make_flag('TopicToken', '--topic')
TimeToken = make_flag('TimeToken', '--time', converter=timestamp13)
GroupIdToken = make_flag('GroupIdToken', '--group', )
OffsetToken = make_flag('OffsetToken', '--offset')
PartitionToken = make_flag('PartitionToken', '--partition')
TimeOutToken = make_flag('TimeOutToken', '--timeout-ms')
FromBeginningToken = make_flag('FromBeginningToken', '--from-beginning')
SkipMessageToken = make_flag('SkipMessageToken', '--skip-message-on-error')
AckRequiredToken = make_flag('AckRequiredToken', '--request-required-acks')
DescribeToken = make_flag('DescribeToken', '--describe')
ListToken = make_flag('ListToken', '--list')
CreateToken = make_flag('CreateToken', '--create')
DeleteToken = make_flag('DeleteToken', '--delete')
ReplicationToken = make_flag('ReplicationToken', '--replication-factor')
PartitionsToken = make_flag('PartitionsToken', '--partitions')
AlterToken = make_flag('AlterToken', '--alter')
AddConfigToken = make_flag('AddConfigToken', '--add-config')
ConsumerPropertiesToken = make_flag('ConsumerPropertiesToken', '--property', repeat=CONSUMER_PROPERTIES)
ProducerPropertiesToken = make_flag('ProducerPropertiesToken', '--property', repeat=PRODUCER_PROPERTIES)


@define(repr=False, slots=False)
class CMDKafkaGetOffsets(CmdRepr):
    bin: str = field(init=False, default='./bin/kafka-get-offsets.sh')
    server: BootstrapServerToken
    topic: TopicToken
    time: TimeToken

    @classmethod
    def build(cls: type, server: str, topic: str, time: Number) -> str:
        return repr(cls(
            BootstrapServerToken(server),
            TopicToken(topic),
            TimeToken(time)
        ))


@define(repr=False, slots=False)
class CMDKafkaConfigs(CmdRepr):
    bin: str = field(init=False, default='./bin/kafka-configs.sh')
    server: BootstrapServerToken
    topic: TopicToken
    alter: AlterToken
    add_config: AddConfigToken

    @classmethod
    def build(cls: type, server: str, topic: str, new_config: str = '') -> str:
        return repr(cls(
            BootstrapServerToken(server),
            TopicToken(topic),
            AlterToken(),
            AddConfigToken(new_config).when(new_config)
        ))


@define(repr=False, slots=False)
class CMDKafkaTopics(CmdRepr):
    bin: str = field(init=False, default='./bin/kafka-topics.sh')
    server: BootstrapServerToken
    topic: TopicToken
    describe: DescribeToken
    list_: ListToken
    create: CreateToken
    delete: DeleteToken
    replication: ReplicationToken
    partitions: PartitionsToken

    @classmethod
    def build(
            cls: type,
            server: str,
            topic: str,
            describe: bool = True,
            list_: bool = False,
            delete: bool = False,
            create: bool = False,
            replication: int = 0,
            partitions: int = 1
    ) -> str:
        return repr(cls(
            BootstrapServerToken(server),
            TopicToken(topic).when(bool(topic)),
            DescribeToken().when(describe),
            ListToken().when(list_),
            DeleteToken().when(delete),
            CreateToken().when(create),
            ReplicationToken(replication).when(create),
            PartitionsToken(partitions).when(create)
        ))


@define(repr=False, slots=False)
class CMDKafkaProducer(CmdRepr):
    bin: str = field(init=False, default='./bin/kafka-console-producer.sh')
    server: BootstrapServerToken
    topic: TopicToken
    ack_required: AckRequiredToken
    properties: ProducerPropertiesToken

    @classmethod
    def build(cls: type, server: str, topic: str, with_key: bool = True) -> str:
        return repr(cls(
            BootstrapServerToken(server),
            TopicToken(topic),
            AckRequiredToken(0),
            ProducerPropertiesToken().when(with_key)
        ))


@define(repr=False, slots=False)
class CMDKafkaConsumer(CmdRepr):
    bin: str = field(init=False, default='./bin/kafka-console-consumer.sh')
    server: BootstrapServerToken
    topic: TopicToken
    offset: OffsetToken
    partition: PartitionToken
    timeout: TimeOutToken
    skipMessage: SkipMessageToken
    properties: ConsumerPropertiesToken
    from_beginning: FromBeginningToken

    @classmethod
    def build(
            cls: type,
            server: str,
            topic: str,
            part: int = -1,
            from_offset: int = -1,
            timeout_ms: int = 2000,
            earliest: bool = True
    ) -> str:
        return repr(cls(
            BootstrapServerToken(server),
            TopicToken(topic),
            OffsetToken(from_offset).when(from_offset >= 0),
            PartitionToken(part).when(part >= 0),
            TimeOutToken(timeout_ms),
            SkipMessageToken(),
            ConsumerPropertiesToken(),
            FromBeginningToken().when(earliest)
        ))


def with_brokers(brokers: str, kafka_cmd: Callable[..., str]) -> Callable[..., str]:
    def builder(*args, **kwargs) -> str:
        return kafka_cmd(brokers, *args, **kwargs)

    return builder


def cmd_producer(key_payload: str, cmd: str) -> str:
    return f"echo '{key_payload}' | {cmd}"
