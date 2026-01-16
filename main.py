import json
import random
import threading
import uuid
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging

from setuptools.config.setupcfg import Target

KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
TOPIC_NAME = 'financial_transactions'
REPLICATION_FACTOR = 3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer_conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 10000,
    'queue.buffering.max.kbytes': 512000,
    'batch.num.messages': 1000,
    'linger.ms': 10,
    'acks': 1,
    'compression.type': 'gzip'
}

producer = Producer(producer_conf)


def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR,
            )

            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic_name}' created successfully")  # Fixed: added closing quote
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic_name}': {e}")

        else:
            logger.info(f"Topic '{topic_name}' already exists!")  # Fixed: added closing quote

    except Exception as e:
        logger.error(f"Error creating Topic: {e}")  # Fixed: {e} instead of (e)


def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=f"user_{random.randint(1, 100)}",
        amount=round(random.uniform(50000, 150000), 2),
        transactionTime=int(time.time()),
        merchantId=random.choice(['merchant_1', 'merchant_2', 'merchant_3']),
        location=f'location_{random.randint(1, 50)}',  # Fixed: was 'transaction', changed to 'location'
        paymentMethod=random.choice(['credit-card', 'paypal', 'bank_transfer']),
        isInternational=random.choice([True, False]),  # Fixed: actual booleans, not strings
        currency=random.choice(['USD', 'EUR', 'GBP'])
    )


def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Delivery failed for record {msg.key()}: {err}')  # Fixed: print → logger, added err
    else:
        logger.info(
            f'Record {msg.key()} successfully delivered to {msg.topic()} [{msg.partition()}]')  # Fixed: typo and added more info



def produce_transaction(thread_id):
    try:
        while True:
            transaction = generate_transaction()

            try:
                producer.produce(
                    topic=TOPIC_NAME,
                    key=transaction['userId'],  # Fixed: was 'userid', should be 'userId'
                    value=json.dumps(transaction).encode('utf-8'),
                    on_delivery=delivery_report
                )
                logger.info(f' Thread {thread_id} Produced transaction: {transaction["transactionId"]}')  # Less verbose logging
                producer.poll(0)  # Added: trigger delivery callbacks

                time.sleep(0.1)  # Added: prevent flooding (10 msgs/sec)

            except BufferError:
                logger.warning('Local producer queue is full, waiting...')
                producer.poll(1)  # Wait for messages to be delivered

            except Exception as e:
                logger.error(f'Error sending transaction: {e}')

    except KeyboardInterrupt:
        logger.info('Shutting down producer...')
    finally:
        producer.flush()  # Ensure all messages are sent before exit
        logger.info('Producer shutdown complete')




def produce_data_in_parrallel(num_threads):
    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transaction,args=(i,))
            thread.daemon=True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except Exception as e:
        print(f'Error message {e}')



if __name__ == "__main__":
    create_topic(TOPIC_NAME)
    produce_data_in_parrallel(3)

