import os
import json
import sqlite3
import xml.etree.ElementTree as ET
import logging

from pathlib import Path
from contextlib import closing
from confluent_kafka import Producer

class StoreKafak:

    Path('logs').mkdir(exist_ok=True)
    logging.basicConfig(
        filename='logs/message.log',
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s"
        )
    
    def __init__(self) -> None:
        self.topic = "StoreKafak"
        self.conf = {
            "bootstrap.servers": "",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "",
            "sasl.password": "",
            "client.id": "store-1"
        }
        self.producer = Producer(self.conf)



    def delivery_callback(self, erorr, message) -> None:
        if erorr:
            logging.error(f"Delivery faild: {erorr}")
        else:
            logging.info(
                f"Delivery to {message.topic()}"
                f"partition = {message.partition()}"
                f"offset = {message.offset()}"
            )


    def send_record(self, key, value) -> None:
        value = json.dumps(value) # > Convert the dictionary into JSON 

        while True:
            try:
                self.producer(
                    self.topic,
                    key = str(key) if key is not None else None,
                    value = value,
                    callback = self.delivery_callback
                )
                break
            except BufferError:
                self.producer.poll(1) # processes available Kafka events and triggers delivery callbacks without waiting.

        self.producer.poll(0)


    def producer_json(self, file_name, key_fild='event_id'):

        with open(f'data/{file_name}', 'r', encoding='unf-8') as lines:
            for line_number, line in enumerate(lines, start=1):

                if not line.strip():
                    continue

                record = json.loads(line)

                if not isinstance(record, dict):
                    assert ValueError(f"the {file_name}:{line_number} file are NOT json file")

                record_key = record.get(key_fild)

                if record_key is None:
                    logging.warning(f"{file_name}:{line_number} missing {key_fild}")

                self.send_record(key=record_key, value=record)

    def producer_xml(self):
        tree = ET.parse("data/courier_deliveries.xml")
        root = tree.getroot()

        for delivery in root.findall(".//delivery"):
            for child in delivery:
                record = {
                    child.tag : child.text,
                }
                self.send_record(key=record.ger("order_id"), value=record)


    def producer_sqllit(self):
        with closing( sqlite3.connect("data/crm_and_catalog.sqlite")) as conn:
            conn.row_factory = sqlite3.Row

            for table in ("customers", "products"):
                cursor = conn.execute(f"SELECT * FROM {table}")

                for row in cursor:
                    record = dict(row)

                    self.send_record(
                        key=record[0],
                        value=record
                    )

    def run_all(self):

        try:
            self.producer_json(file_name="web_orders.jsonl")
            self.producer_json(file_name="web_activity.jsonl")
            self.producer_json(file_name="returns.jsonl")
            self.producer_json(file_name="payments.jsonl")
            self.producer_json(file_name="mobile_orders.jsonl")
            self.producer_json(file_name="inventory.jsonl")

            self.producer_sqllit()
            self.producer_xml()

        finally:
            remaining = self.producer.flush(30)
            if remaining:
                logging.warning(f"{remaining} messages remain undelivered")
            else:
                logging.info("All Jobs finished")
        