from pyclients.cmd.command import make_flag, token, cmd, CmdType
from pyclients.utils import timestamp13
from pyclients.abc.types import Number

# when changing config for all brokers:
# kafka-configs.sh --bootstrap-server localhost:9092 --entity-type brokers --entity-default --alter --add-config ...

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
GroupIdToken = make_flag('GroupIdToken', '--group')
OffsetToken = make_flag('OffsetToken', '--offset')
PartitionToken = make_flag('PartitionToken', '--partition')
TimeOutToken = make_flag('TimeOutToken', '--timeout-ms')
FromBeginningToken = make_flag('FromBeginningToken', '--from-beginning')
SkipMessageToken = make_flag('SkipMessageToken', '--skip-message-on-error')
AckRequiredToken = make_flag('AckRequiredToken', '--request-required-acks')
DescribeToken = make_flag('DescribeToken', '--describe')
ListToken = make_flag('ListToken', '--list')
HelpToken = make_flag('HelpToken', '--help')
CreateToken = make_flag('CreateToken', '--create')
DeleteToken = make_flag('DeleteToken', '--delete')
ReplicationToken = make_flag('ReplicationToken', '--replication-factor')
PartitionsToken = make_flag('PartitionsToken', '--partitions')
AlterToken = make_flag('AlterToken', '--alter')
AddConfigToken = make_flag('AddConfigToken', '--add-config')
ConsumerPropertiesToken = make_flag('ConsumerPropertiesToken', '--property', repeat=CONSUMER_PROPERTIES)
ProducerPropertiesToken = make_flag('ProducerPropertiesToken', '--property', repeat=PRODUCER_PROPERTIES)


@cmd('./bin/kafka-get-offsets.sh')
class CMDKafkaGetOffsets:
    server: str = token(lambda self, s: BootstrapServerToken(s))
    topic: str = token(lambda self, s: TopicToken(s))
    timestamp: Number = token(lambda self, n: TimeToken(n))


@cmd('./bin/kafka-configs.sh')
class CMDKafkaConfigs:
    server: str = token(lambda self, s: BootstrapServerToken(s))
    help: bool = token(lambda self, b: HelpToken().when(b), base=False)
    topic: str = token(lambda self, s: TopicToken(s).when(not self.help), base='')
    alter: str = token(lambda self, s: AlterToken().when(not self.help), boolean_flag=True)
    add_config: str = token(lambda self, s: AddConfigToken(s).when(s and not self.help), base='')


@cmd('./bin/kafka-topics.sh')
class CMDKafkaTopics:
    server: str = token(lambda self, s: BootstrapServerToken(s))
    topic: str = token(lambda self, s: TopicToken(s).when(bool(s)))
    describe: bool = token(lambda self, b: DescribeToken().when(b), base=True)
    list_: bool = token(lambda self, b: ListToken().when(b), base=False)
    delete: bool = token(lambda self, b: DeleteToken().when(b), base=False)
    create: bool = token(lambda self, b: CreateToken().when(b), base=False)
    alter: bool = token(lambda self, b: AlterToken().when(b), base=False)
    replication: int = token(lambda self, n: ReplicationToken(n).when(self.create), base=0)
    partitions: int = token(lambda self, n: PartitionsToken(n).when(self.create or self.alter), base=1)


@cmd('./bin/kafka-console-producer.sh')
class CMDKafkaProducer:
    server: str = token(lambda self, s: BootstrapServerToken(s))
    topic: str = token(lambda self, s: TopicToken(s))
    ack_required: int = token(lambda self, n: AckRequiredToken(n), base=0)
    with_key: bool = token(lambda self, b: ProducerPropertiesToken().when(b), base=True)


@cmd('./bin/kafka-console-consumer.sh')
class CMDKafkaConsumer:
    server: str = token(lambda self, s: BootstrapServerToken(s))
    topic: str = token(lambda self, s: TopicToken(s))
    from_offset: int = token(lambda self, n: OffsetToken(n).when(n >= 0), base=-1)
    partition: int = token(lambda self, n: PartitionToken(n).when(n >= 0), base=-1)
    timeout_ms: int = token(lambda self, n: TimeOutToken(n), base=2000)
    skipMessage: bool = token(lambda self, b: SkipMessageToken(), boolean_flag=True)
    properties: bool = token(lambda self, b: ConsumerPropertiesToken(), boolean_flag=True)
    from_beginning: bool = token(lambda self, b: FromBeginningToken().when(b), base=True)


def cmd_producer(key_payload: str, cmd: str) -> str:
    return f"echo '{key_payload}' | {cmd}"
