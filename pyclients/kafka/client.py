import re
import time

from attrs import define
from toolz import identity, excepts
from fabric import Connection
from typing import Optional, Any
from loguru import logger

from pyclients.connection.bridge import ConnectionBridge
from pyclients.abc.types import MaybeNumber, RecordPredicate, PayloadMapper, StringPredicate
from pyclients.kafka.commands import CMDKafkaGetOffsets, CMDKafkaConsumer, CMDKafkaProducer, CMDKafkaTopics, \
    CMDKafkaConfigs, cmd_producer
from pyclients.kafka.model import TopicPartitionOffset, ConsumerRecord, ProducerRecord, PartitionReplicaConfig
from pyclients.utils import secure, timestamp13, inspect_call, last_, now_with_delta, join_posix, \
    fmt_timestamp
from pyclients.abc.client import PyClient


@define
class KafkaSSH(PyClient, ConnectionBridge):
    home: str = "$HOME"
    kafka_home_path: str = ""
    kafka_tar_file_path: str = ""
    brokers: str = 'localhost:9092'
    client_tar_url: str = 'https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz'
    show: bool = True
    # pass valid fabric connection for remote ssh
    conn: Optional[Connection] = None

    @inspect_call(arguments=False, return_val=True)
    def __attrs_post_init__(self):
        tar_url_parser = r'(?P<tar>(?P<version>kafka[-_][.\-0-9]+\d)[^/]*.tgz)'
        match = re.search(tar_url_parser, self.client_tar_url)
        version_dir = match.group('version') if match else ''
        alt_tar = join_posix(self.home, version_dir, match.group('tar'))

        self.kafka_home_path = self.kafka_home_path or join_posix(self.home, version_dir)
        self.kafka_tar_file_path = self.kafka_tar_file_path or alt_tar

        ConnectionBridge.__init__(self, conn=self.conn or self.local_connection)
        return self

    @secure()
    def connect(self) -> None:
        self.open()
        self._download_client()
        self.set_origin(self.kafka_home_path)

    @secure(alt_value=False)
    def test_connection(self) -> bool:
        client_ready = self.run_basic(f"test -d {self.kafka_home_path}/bin").ok
        return client_ready and self.connected()

    def close_connection(self) -> None:
        self.close()

    def query_topic(
            self,
            topic: str,
            from_timestamp: MaybeNumber = None,
            to_timestamp: MaybeNumber = None,
            from_delta: str = None,
            to_delta: str = None,
            idle_time_ms: int = 4000,
            filter_in: RecordPredicate = None,
            payload_mapper: PayloadMapper = None,
            # for optimization purposes
            stop_consumer_on: StringPredicate = None,
            sub_command: str = ''
    ) -> list[Any]:
        """
        Fetch messages from topic within the given time range.

        :param topic: topic to query.
        :param from_timestamp: 10 or 13 digits. Has privilege over `from_delta`.
        :param to_timestamp: 10 or 13 digits. Has privilege over `to_delta`.
        :param from_delta: follows '\d+[dhms]' pattern (day, hour, minute, second).
        :param to_delta: see `from_delta`.
        :param idle_time_ms: max time consumer will wait for messages to be read.
        :param filter_in: predicate to filter records.
        :param payload_mapper: converter applied to all records.
        :param stop_consumer_on: (for optimization only, not needed). Consumer will be abruptly terminated if
            this predicate returns true for the current monitored stdout. Seen records will be returned.
        :param sub_command: (for optimization only, not needed). Additional command to be appended to the main
            kafka consumer terminal command. It can help by reducing and/or eliminating unnecessary data return
            by the server process, e.g., adding 'sed' commands to filter out messages.

        :return: list of ConsumerRecord or any object (when `payload_mapper` is applied).
        """
        from_delta = from_delta and now_with_delta(from_delta)
        to_delta = to_delta and now_with_delta(to_delta)
        from_ts = timestamp13(from_timestamp or from_delta or 1262304000000)
        to_ts = timestamp13(to_timestamp or to_delta or time.time())

        from_ts, to_ts = (from_ts, to_ts) if to_ts > from_ts else (to_ts, from_ts)
        logger.debug("from_delta: {}, to_delta: {}, from_ts: {}, to_ts: {}", from_delta, to_delta, from_ts, to_ts)
        logger.info(f'Query time boundaries: from {fmt_timestamp(from_ts)} to {fmt_timestamp(to_ts)}')

        get_offsets_for = lambda num: CMDKafkaGetOffsets(self.brokers, topic, timestamp=num).get()
        offsets_earliest = self.run_basic(get_offsets_for(from_ts)).stdout
        offsets_latest = self.run_basic(get_offsets_for(to_ts)).stdout
        offsets_last_available = self.run_basic(get_offsets_for(-1)).stdout

        offsets_info = [
            (TopicPartitionOffset(tpo1.topic, tpo1.partition, tpo1.offset), total)
            for tpo1 in TopicPartitionOffset.from_lines(offsets_earliest)
            for tpo2 in TopicPartitionOffset.from_lines(offsets_latest)
            for tpo3 in TopicPartitionOffset.from_lines(offsets_last_available)
            if (
                    tpo1.partition == tpo2.partition == tpo3.partition and
                    isinstance(tpo1.offset, int) and
                    (max_offset := tpo2.offset or tpo3.offset or 0) and
                    (total := abs(max_offset - tpo1.offset)) > 0
            )
        ]

        csm_args = {'server': self.brokers, 'topic': topic, 'from_beginning': False, 'timeout_ms': idle_time_ms}
        from_pair = lambda p: {'partition': p[0].partition, 'from_offset': p[0].offset, 'max_messages': p[1]}
        consumer_cmds = [CMDKafkaConsumer(**csm_args, **from_pair(pair)).get() for pair in offsets_info]

        records = []
        if consumer_cmds:
            stdout_lines = self.run_async2(consumer_cmds, force_termination=stop_consumer_on, suffix=sub_command)
            records = [ConsumerRecord.from_string(rec) for rec in stdout_lines]
            logger.debug(f"stdoutLinesLen: '{len(stdout_lines)}', recordsParsed '{len(records)}'")

        return [
            (payload_mapper or identity)(record)
            for record in records
            if record and excepts(Exception, lambda rec: filter_in(rec) if filter_in else True)(record)
        ]

    def produce(self, topic: str, record: ProducerRecord):
        """Produce record."""
        topics = self.list_topics()

        if topic not in topics:
            logger.info('Topic not found!')
            return

        cmd = cmd_producer(record.to_raw(), CMDKafkaProducer(self.brokers, topic, with_key=bool(record.key)).get())
        result = self.run_basic(cmd)
        logger.info(f"Producer record created ({record}): {result.ok}")

    def alter_topic(self, topic: str, config: str = '', recreate: bool = False) -> None:
        """
        Alter, or recreate Topic. Topic must already exist for actions to take place:
        - If `recreate` is False and not `config` is passed then show topic info only.
        - If `recreate` is False and `config` is passed then update topic config.
        - If `recreate` is True topic is deleted and created again with older + updated config merged.

        NOTE: If replication factor is passed (replication=n) or new partition number passed is smaller than
        current partition number topic will be recreated.

        :param topic: topic name.
        :param config: new set of config values. Must be comma separated, e.g., 'segment.bytes=100,retention.ms=100'.
            You can pass `partitions` and `replication` keys in 'config' if you want to alter these values,
            e.g., 'partitions=20,replication=3'.
        :param recreate: if True topic is deleted and created again with the same config values. If `config` is
            passed then values will be updated.
        """
        new_partitions = 'partitions=' in config
        new_replication = 'replication=' in config
        topics = self.list_topics()

        if topic not in topics:
            logger.info('Topic not found!')
            return

        desc = self.run_basic(CMDKafkaTopics(self.brokers, topic, describe=True).get()).stdout
        pr_conf = PartitionReplicaConfig.from_string(desc)
        new_config = f"{pr_conf.comb()},{config}"
        parts, _, config_ = parse_topic_config(new_config)
        smaller_partition = parts < pr_conf.partition
        logger.info(f"Original configuration found for '{topic}': {pr_conf}")

        if recreate or new_replication or smaller_partition:
            self.delete_topic(topic)
            self.create_topic(topic, config=new_config)
            return

        elif config:
            cmd = CMDKafkaTopics(self.brokers, topic, describe=False, alter=True, partitions=parts)
            new_partitions and self.run_basic(cmd.get())
            self.run_basic(CMDKafkaConfigs(self.brokers, topic=topic, add_config=config_).get(), hide=self.show)

        self.run_basic(CMDKafkaTopics(self.brokers, topic, describe=True).get(), hide=self.show)

    def create_topic(self, topic: str, config: str = ''):
        """
        Create topic; topic description is printed.

        :param topic: topic to be created (only if it doesn't already exist).
        :param config: topic configuration (run 'config_help' to see options). To modify partitions and replica
            factor use 'partitions' and 'replication'. Config Example: 'partitions=2,replica=1,flush.ms=8000'.
        """
        topics = self.list_topics()
        parts, replicas, cfg = parse_topic_config(config)

        if topic in topics:
            logger.info('Topic already exists!')
        else:
            args = {'describe': False, 'create': True, 'partitions': parts, 'replication': replicas}
            self.run_basic(CMDKafkaTopics(self.brokers, topic, **args).get(), hide=self.show)
            cfg and self.run_basic(CMDKafkaConfigs(self.brokers, topic=topic, add_config=cfg).get(), hide=self.show)

        self.run_basic(CMDKafkaTopics(self.brokers, topic, describe=True).get(), hide=self.show)

    def delete_topic(self, topic: str):
        """Delete topic."""
        result = self.run_basic(CMDKafkaTopics(self.brokers, topic, describe=False, list_=False, delete=True).get())
        logger.info(f"Topic '{topic}' deleted: {result.ok}")

    def list_topics(self) -> list[str]:
        """List all topics."""
        topics = self.run_async2(CMDKafkaTopics(self.brokers, topic='', describe=False, list_=True).get())
        return [t for t in topics if 'PID:' not in t]

    def config_help(self):
        """Simply print the kafka-configs.sh menu."""
        self.run_basic(CMDKafkaConfigs(self.brokers, help=True).get(), hide=False)

    def _download_client(self):
        """
        Download and untar kafka client inside `kafka_home_path` directory. Downloads only if tar file
        `kafka_tar_file_path` is not found.
        """
        no_kafka_home = self.run_basic(f"test -d {self.kafka_home_path}").failed
        no_kafka_tar = self.run_basic(f"test -f {self.kafka_tar_file_path}").failed
        logger.debug(f'Kafka home found: {not no_kafka_home}. Tar file found: {not no_kafka_tar}')

        if no_kafka_home and no_kafka_tar:
            self.run_basic(f"mkdir -p {self.kafka_home_path}")
            self.run_basic(f"cd {self.kafka_home_path} && wget {self.client_tar_url}")
            self.run_basic(f"cd {self.kafka_home_path} && tar -xzvf *.tgz --strip-components=1")

        elif no_kafka_home:
            self.run_basic(f"mkdir -p {self.kafka_home_path}")
            self.run_basic(f"tar -xzvf {self.kafka_tar_file_path} -C {self.kafka_home_path} --strip-components=1")


def parse_topic_config(config: str, def_partitions=1, def_replicas=1) -> (int, int, str):
    """
    Remove `partitions` and `replication` from config string (they are not official config parameters). Return
    defaults if values not found.
    """
    partitions = int(last_(re.findall(r'partitions=(\d+)', config)) or def_partitions)
    replicas = int(last_(re.findall(r'replication=(\d+)', config)) or def_replicas)

    config_ = re.sub(r'partitions=\d+|replication=\d+', '', config)
    config_ = re.sub(r' +|^,+|,+$', '', config_)
    config_ = re.sub(r',+', ',', config_)

    return partitions, replicas, config_
