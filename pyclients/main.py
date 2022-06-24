from pyclients.kafka.client import KafkaSSH


def test_kakfa():
    kafka = KafkaSSH(kafka_home_path='~/Desktop/kafka_2.13-3.0.0')
    kafka.connect()
    print(kafka.list_topics())
