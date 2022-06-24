import re
import os
import time

from attrs import define
from toolz import identity, excepts
from fabric import Connection
from typing import Optional, Any
from loguru import logger

from pyclients.cmd.helpers import cmd_builder
from pyclients.connection.bridge import ConnectionBridge
from pyclients.abc.types import MaybeNumber, RecordPredicate, PayloadMapper
from pyclients.kafka.commands import CMDKafkaGetOffsets, CMDKafkaConsumer, CMDKafkaProducer, CMDKafkaTopics, \
    CMDKafkaConfigs, cmd_producer
from pyclients.kafka.model import TopicPartitionOffset, ConsumerRecord, ProducerRecord, PartitionReplicaConfig
from pyclients.utils import secure, timestamp13, inspect_call, last_, regx_plus, now_with_delta
from pyclients.abc.client import PyClient


@define
class KafkaSSH(PyClient, ConnectionBridge):
    home: str = "$HOME"
    kafka_home_path: str = ""
    kafka_tar_file_path: str = ""
    brokers: str = 'localhost:9092'
    client_tar_url: str = 'https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz'
    # pass valid fabric connection for remote ssh
    conn: Optional[Connection] = None

    @inspect_call(return_val=True)
    def __attrs_post_init__(self):
        tar_url_parser = r'(?P<tar>(?P<version>kafka[-_][.\-0-9]+\d)[^/]*.tgz)'
        match = re.search(tar_url_parser, self.client_tar_url)
        version_dir = match.group('version') if match else ''
        alt_tar = os.path.join(self.home, version_dir, match.group('tar'))

        self.kafka_home_path = self.kafka_home_path or os.path.join(self.home, version_dir)
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
        client_ready = self.run_(f"test -d {self.kafka_home_path}/bin", warn=True).ok
        return client_ready and self.connected()

    def close_connection(self) -> None:
        self.close()

    def consume(
            self,
            topic: str,
            to_timestamp: MaybeNumber = None,
            to_delta: str = None,
            partition_strategy: bool = False,
            from_timestamp: MaybeNumber = None,
            from_delta: str = None,
            idle_time_ms: int = 3000,
            filter_in: RecordPredicate = lambda record: True,
            payload_mapper: PayloadMapper = identity
    ) -> list[Any]:
        """
        Consume Kafka messages.

        :param topic: kafka topic.
        :param to_timestamp: stop consumption when message timestamp >= 'to_timestamp'.
        :param to_delta: now - 'to_delta'. Format: '\d+[dhms]', where d=day, h=hour, m=minute, s=second.
        :param partition_strategy: consume asynchronously from each topic partition.
        :param from_timestamp: starting consumption from this timestamp. Only used when by_partition_strategy=True.
        :param from_delta: now - 'from_delta' (See 'to_delta'). Only used when by_partition_strategy=True.
        :param idle_time_ms: waiting time to fetch messages. Consumer exits if no message is fetched.
        :param filter_in: predicate to filter messages.
        :param payload_mapper: function maps consumer record to any object.

        :return: list of consumer records or any object if passing a 'payload_mapper'.
        """
        from_delta = from_delta and now_with_delta(from_delta)
        to_delta = to_delta and now_with_delta(to_delta)
        from_timestamp = timestamp13(from_timestamp or from_delta or 1262304000000)
        max_timestamp = timestamp13(to_timestamp or to_delta or time.time())
        logger.debug(f'Time boundaries: from {from_timestamp} to {max_timestamp}')

        time_limit = lambda line_str: regx_plus(line_str, r'CreateTime:(\d*)', 1, int) or -1 >= max_timestamp
        consumer_cmd = cmd_builder(CMDKafkaConsumer, server=self.brokers, topic=topic, timeout_ms=idle_time_ms)

        if not partition_strategy:
            stdout_all = self.run(consumer_cmd(), stopper=time_limit).stdout()
        else:
            cmd = CMDKafkaGetOffsets(self.brokers, topic, from_timestamp)
            offset_for_partitions = self.run(cmd.get()).stdout()
            consumer_cmds = [
                consumer_cmd(partition=t3.partition, from_offset=t3.offset, from_beginning=False)()
                for t3 in TopicPartitionOffset.from_lines(offset_for_partitions)
            ]

            stdout_all = self.run_async(consumer_cmds, stopper=time_limit).stdout()

        records = ConsumerRecord.from_lines(stdout_all)
        logger.debug(f"stdout_all: '{stdout_all[:100]}'... '{len(records)}' records parsed.")

        return [
            payload_mapper(record)
            for record in records
            if record and excepts(Exception, lambda rec: filter_in(rec))(record)
        ]

    def produce(self, topic: str, record: ProducerRecord):
        topics = self.list_topics()

        if topic not in topics:
            logger.info('Topic not found!')
            return

        cmd = cmd_producer(record.to_raw(), CMDKafkaProducer(self.brokers, topic, with_key=bool(record.key)).get())
        result = self.run(cmd, hide=False)
        logger.info(f"Producer record created ({record}): {result.result.ok}")

    def alter_topic(self, topic: str, config: str = '', recreate: bool = False) -> None:
        """
        Alter, or recreate Topic. Topic must already exist:
        - If `recreate` is False and not `config` is passed then show topic info only.
        - If `recreate` is False and `config` is passed then update topic config.
        - If `recreate` is True topic is deleted and created again with older + updated config merged.

        NOTE: If replication factor is passed (replication=n) topic will be recreated.

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

        desc = self.run(CMDKafkaTopics(self.brokers, topic, describe=True).get(), hide=True).stdout()
        pr_conf = PartitionReplicaConfig.from_string(desc)
        new_config = f"{pr_conf.comb()},{config}"
        logger.info(f"Original configuration found for '{topic}': {pr_conf}")

        if recreate or new_replication:
            self.delete_topic(topic)
            self.create_topic(topic, config=new_config)
            return

        elif config:
            parts, _, config_ = parse_topic_config(new_config)
            cmd = CMDKafkaTopics(self.brokers, topic, describe=False, alter=True, partitions=parts)
            new_partitions and self.run(cmd.get(), hide=False)
            self.run(CMDKafkaConfigs(self.brokers, topic=topic, add_config=config_).get(), hide=False)

        self.run(CMDKafkaTopics(self.brokers, topic, describe=True).get(), hide=False)

    def create_topic(self, topic: str, config: str = ''):
        """
        Creates topic. Topic description is printed.

        :param topic: topic to be created (only if it doesn't already exists).
        :param config: topic configuration (run 'config_help' to see options). To modify partitions and replica
            factor use 'partitions' and 'replication'. Config Example: 'partitions=2,replica=1,flush.ms=8000'.
        """
        topics = self.list_topics()
        parts, replicas, config_ = parse_topic_config(config)

        if topic in topics:
            logger.info('Topic already exists!')
        else:
            args = {'describe': False, 'create': True, 'partitions': parts, 'replication': replicas}
            self.run(CMDKafkaTopics(self.brokers, topic, **args).get(), hide=False)
            config_ and self.run(CMDKafkaConfigs(self.brokers, topic=topic, add_config=config_).get(), hide=False)

        self.run(CMDKafkaTopics(self.brokers, topic, describe=True).get(), hide=False)

    def delete_topic(self, topic: str):
        result = self.run(CMDKafkaTopics(self.brokers, topic, describe=False, list_=False, delete=True).get())
        logger.info(f"Topic '{topic}' deleted: {result.result.ok}")

    def list_topics(self) -> list[str]:
        return self.run(
            CMDKafkaTopics(self.brokers, topic='', describe=False, list_=True).get()
        ).lines(lambda t: 'PID:' not in t)

    def config_help(self):
        self.run(CMDKafkaConfigs(self.brokers, help=True).get(), hide=False)

    def _download_client(self):
        """
        Download and untar kafka client inside `kafka_home_path` directory.
        If `kafka_tar_file_path` was found then only untar that file.
        """
        no_kafka_home = self.run_(f"test -d {self.kafka_home_path}", warn=True).failed
        no_kafka_tar = self.run_(f"test -f {self.kafka_tar_file_path}", warn=True).failed
        logger.debug(f'Kafka home found: {not no_kafka_home}. Tar file found: {not no_kafka_tar}')

        if no_kafka_home and no_kafka_tar:
            self.run_(f"mkdir -p {self.kafka_home_path}")
            self.run_(f"cd {self.kafka_home_path} && wget {self.client_tar_url}")
            self.run_(f"cd {self.kafka_home_path} && tar -xzvf *.tgz --strip-components=1")

        elif no_kafka_home:
            self.run_(f"mkdir -p {self.kafka_home_path}")
            self.run_(f"tar -xzvf {self.kafka_tar_file_path} -C {self.kafka_home_path} --strip-components=1")


def parse_topic_config(config: str, def_partitions=1, def_replicas=1) -> (int, int, str):
    partitions = int(last_(re.findall(r'partitions=(\d+)', config)) or def_partitions)
    replicas = int(last_(re.findall(r'replication=(\d+)', config)) or def_replicas)

    config_ = re.sub(r'partitions=\d+|replication=\d+', '', config)
    config_ = re.sub(r' +|^,+|,+$', '', config_)
    config_ = re.sub(r',+', ',', config_)

    return partitions, replicas, config_

