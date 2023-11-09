#!/usr/bin/env python
"""OTUS BigData ML kafka producer example"""

import json
from typing import Dict, NamedTuple
import logging
import numpy as np
import argparse
from collections import namedtuple

import kafka

MAX_USER_ID = 100
MAX_PAGE_ID = 10


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    offset: int


def main():
    argparser = argparse.ArgumentParser(description=__doc__)
    argparser.add_argument(
        "-b",
        "--bootstrap_server",
        required=True
        help="kafka server address:port",
    )
    argparser.add_argument(
        "-u", "--user", default="mlops", help="kafka user"
    )
    argparser.add_argument(
        "-p", "--password", default="mlops_pw", help="kafka user password"
    )
    argparser.add_argument(
        "-t", "--topic", default="test", help="kafka topic to consume"
    )
    argparser.add_argument(
        "-n",
        default=10,
        type=int,
        help="number of messages to send",
    )

    args = argparser.parse_args()

    producer = kafka.KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=args.user,
        sasl_plain_password=args.password,
        ssl_cafile="YandexCA.crt",
        value_serializer=serialize,
    )

    generate(producer, args)


def generate(producer, args):

    try:
        for i in range(args.n):
            record_md = send_message(producer, args.topic)
            print(
                f"Msg sent. Topic: {record_md.topic}, partition:{record_md.partition}, offset:{record_md.offset}"
            )
    except kafka.errors.KafkaError as err:
        logging.exception(err)
    producer.flush()
    producer.close()


def send_message(producer: kafka.KafkaProducer, topic: str) -> RecordMetadata:
    data = data_generator()
    future = producer.send(
        topic=topic,
        # key=str(click["page_id"]).encode("ascii"),
        value=data,
    )

    # Block for 'synchronous' sends
    record_metadata = future.get()
    return RecordMetadata(
        topic=record_metadata.topic,
        partition=record_metadata.partition,
        offset=record_metadata.offset,
    )


def data_generator():
    a = np.random.normal(loc=1, scale=2)
    b = np.random.normal(loc=-1, scale=2)
    c = np.random.normal(loc=5, scale=2)
    x1 = np.random.randint(-100, 100)
    x2 = np.random.randint(-100, 100)
    x3 = np.random.randint(-100, 100)
    return {'X': [2 * (a + x1), 3 * (b + x2), x3], 'y': x1 + x2 + x3}


def serialize(msg: Dict) -> bytes:
    return json.dumps(msg).encode("utf-8")



if __name__ == "__main__":
    main()
