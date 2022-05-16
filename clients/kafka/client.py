import re
import os
import time

from attrs import define, field
from toolz import identity, excepts
from fabric import Connection, Result
from typing import Optional
from loguru import logger

from clients.connection import ConnectionBridge
from clients.interface.types import MaybeNumber, RecordPredicate, PayloadMapper
from clients.kafka.commands import CMDKafkaGetOffsets, CMDKafkaConsumer, CMDKafkaProducer, CMDKafkaTopics, \
    CMDKafkaConfigs, with_brokers, cmd_producer
from clients.kafka.model import TopicPartitionOffset, ConsumerRecord, ProducerRecord, PartitionReplicaConfig
from clients.kafka.stream_watcher import KafkaStreamWatcher
from clients.utils import secure, timestamp13, inspect_func, to_int, last_
from clients.interface.client import Client


@define
class KafkaSSH(Client):
    # use valid ssh connection for remote
    conn: Optional[Connection] = None
    home: str = "$HOME"
    kafka_home_path: str = ""
    kafka_tar_file_path: str = ""
    brokers: str = 'localhost:9092'
    client_tar_url: str = 'https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz'
    clt: ConnectionBridge = field(init=False, default=ConnectionBridge())

    @inspect_func(return_val=True)
    def __attrs_post_init__(self):
        tar_url_parser = r'(?P<tar>(?P<version>kafka[-_][.\-0-9]+\d)[^/]*.tgz)'
        match = re.search(tar_url_parser, self.client_tar_url)
        version_dir = match.group('version') if match else ''
        alt_tar = os.path.join(self.home, version_dir, match.group('tar'))

        self.kafka_home_path = self.kafka_home_path or os.path.join(self.home, version_dir)
        self.kafka_tar_file_path = self.kafka_tar_file_path or alt_tar

        self.clt = self.clt if not self.conn else ConnectionBridge(self.conn)
        self.clt.set_origin(self.kafka_home_path)
        self.clt.extend_holder(self)
        self.conn = None

        return self

    def consume(
            self,
            topic: str,
            from_timestamp: MaybeNumber = None,
            to_timestamp: MaybeNumber = None,
            idle_time_ms: int = 3000,
            by_partition_strategy: bool = False,
            query: RecordPredicate = lambda record: True,
            payload_mapper: PayloadMapper = identity
    ):
        from_timestamp = from_timestamp or 1262304000000
        to_timestamp = timestamp13(to_timestamp or time.time())
        offset_seek_cmd = with_brokers(self.brokers, CMDKafkaGetOffsets.build)
        consume_cmd = with_brokers(self.brokers, CMDKafkaConsumer.build)
        watcher = KafkaStreamWatcher(self.clt, to_timestamp)

        if not by_partition_strategy:
            cmd = consume_cmd(topic, timeout_ms=idle_time_ms)
            self.run_buffered(cmd, warn=True, watchers=[watcher])
        else:
            self.run_buffered(offset_seek_cmd(topic, from_timestamp))

            for triple in TopicPartitionOffset.from_lines(self.clt.drain_buffer()):
                cmd = consume_cmd(topic, triple.partition, triple.offset, idle_time_ms, earliest=False)
                self.run_buffered(cmd, warn=True, watchers=[watcher])

        return [
            record.map_payload(payload_mapper)
            for record in ConsumerRecord.from_lines(self.clt.drain_buffer())
            if record and excepts(Exception, lambda rec: query(rec))(record)
        ]

    def produce(self, topic: str, record: ProducerRecord):
        cmd = cmd_producer(record.to_raw(), CMDKafkaProducer.build(self.brokers, topic, bool(record.key)))

        self.run_with(cmd, hide=True)
        logger.info(f"Producer record created: {record}")

    def alter_topic(self, topic: str, config: str = '', recreate: bool = True) -> None:
        """
        Alter, create, or recreate Topic:
        - If topic doesn't exist it is created (regardless of `recreate`).
        - If `recreate` is False and not `config` is passed then show topic info only (if it exists).
        - If `recreate` is False and `config` is passed then update topic config (if it exists).
        - If `recreate` is True topic is deleted and created again with older + updated config.

        :param topic: topic name.
        :param config: new set of config values. Must be comma separated, e.g., 'segment.bytes=100,retention.ms=100'.
            You can pass `partitions` and `replication` keys in 'config' if you want to alter these values,
            e.g., 'partitions=20,replication=3'.
        :param recreate: if True topic is deleted and created again with the same config values. If `config` is
            passed then values will be updated with new list.
        :return: None
        """
        topic_partial_cmd = with_brokers(self.brokers, CMDKafkaTopics.build)
        config_partial_cmd = with_brokers(self.brokers, CMDKafkaConfigs.build)
        describe_cmd = topic_partial_cmd(topic, describe=True, list_=False, delete=False)
        list_cmd = topic_partial_cmd('', describe=False, list_=True, delete=False)
        delete_cmd = topic_partial_cmd(topic, describe=False, list_=False, delete=True)

        # gets the latest partitions and replication value
        partitions = to_int(last_(re.findall(r'partitions=(\d+)', config)) or 1)
        replicas = to_int(last_(re.findall(r'replication=(\d+)', config)) or 1)
        create_cmd = topic_partial_cmd(topic, describe=False, create=True, replication=replicas, partitions=partitions)

        config_ = re.sub(r'partitions=\d+|replication=\d+', '', config)
        config_ = re.sub(r' |^,+|,+$|partitions=\d+|replication=\d+', '', config_)
        config_ = re.sub(r',+', ',', config_)
        config_cmd = config_partial_cmd(topic, new_config=config_)

        topics: str = self.run_with(list_cmd, hide=True).stdout
        if topic not in topics:
            self.run_with(create_cmd)
            config_ and self.run_with(config_cmd)
            return

        if not recreate and not config_:
            self.run_with(describe_cmd)

        elif not recreate and config_:
            self.run_with(config_cmd)

        elif recreate:
            prc = PartitionReplicaConfig.from_string(self.run_with(describe_cmd, hide=True).stdout)
            self.run_with(delete_cmd, hide=True)
            config = f"partitions={prc.partition},replication={prc.replica_factor},{prc.config},{config}"
            return self.alter_topic(topic, config)

    def delete_topic(self, topic: str):
        topic_partial_cmd = with_brokers(self.brokers, CMDKafkaTopics.build)
        delete_cmd = topic_partial_cmd(topic, describe=False, list_=False, delete=True)
        self.run_with(delete_cmd, warn=True)
        logger.debug(f"Topic '{topic}' deleted.")

    def list_topics(self):
        topic_partial_cmd = with_brokers(self.brokers, CMDKafkaTopics.build)
        list_cmd = topic_partial_cmd('', describe=False, list_=True, delete=False)
        self.run_with(list_cmd)

    @secure()
    def connect(self) -> None:
        self.clt.open()
        self._download_client()

    @secure(alt_value=False)
    def test_connection(self) -> bool:
        client_ready = self.run(f"test -d {self.kafka_home_path}", warn=True).ok
        return client_ready and self.clt.connected()

    def close(self) -> None:
        self.clt.close()

    def _download_client(self):
        no_kafka_home = self.run(f"test -d {self.kafka_home_path}", warn=True).failed
        no_kafka_tar = self.run(f"test -f {self.kafka_tar_file_path}", warn=True).failed
        logger.debug(f'Kafka home found: {not no_kafka_home}. Tar file found: {not no_kafka_tar}')

        if no_kafka_home and no_kafka_tar:
            self.run(f"mkdir -p {self.kafka_home_path}")
            self.run(f"cd {self.kafka_home_path} && wget {self.client_tar_url}")
            self.run(f"cd {self.kafka_home_path} && tar -xzvf *.tgz --strip-components=1")

        elif no_kafka_home:
            self.run(f"mkdir -p {self.kafka_home_path}")
            self.run(f"tar -xzvf {self.kafka_tar_file_path} -C {self.kafka_home_path} --strip-components=1")
