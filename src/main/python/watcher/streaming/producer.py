import json
import logging

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka.errors import KafkaError


class Producer:
    config_dict = {}

    @classmethod
    def push(cls, message: dict, logger: logging.Logger) -> bool:
        record_metadata = None
        producer = KafkaProducer(bootstrap_servers=cls.config_dict["bootstrap.server"],
                                 security_protocol=cls.config_dict["security.protocol"],
                                 sasl_mechanism=cls.config_dict["sasl.mechanism"],
                                 sasl_plain_username=cls.config_dict["sasl.username"],
                                 sasl_plain_password=cls.config_dict["sasl.password"])
        future = producer.send(cls.config_dict["topic.name"],
                               json.dumps(message, separators=(",", ":")).encode("utf-8"))

        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as ke:
            logger.error(f"KafkaError: '{ke}'")
            return False

        logger.info(f"Kafka: '{record_metadata}'")

        """
        (producer.send(cls.config_dict["topic.name"], json.dumps(message, separators=(",", ":")).encode("utf-8"))
         .add_callback(cls.on_send_success)
         .add_errback(cls.on_send_error))
        """

        return True

    @classmethod
    def on_send_success(cls, record_metadata, logger: logging.Logger):
        logger.info(f"record_metadata: '{record_metadata}'")
        return True

    @classmethod
    def on_send_error(cls, ex, logger: logging.Logger):
        logger.info(f"Error: '{ex}'")
        return False

    @classmethod
    def read_config(cls, path):
        with open(path, mode="r", encoding="utf-8") as fp:
            cls.config_dict = json.load(fp)
