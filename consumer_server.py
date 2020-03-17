import asyncio
import json

from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    #
    # TODO: Create a CachedSchemaRegistryClient
    #
    schema_registry = CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL)

    #
    # TODO: Use the Avro Consumer See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html
    #  ?highlight=loads#confluent_kafka.avro.AvroConsumer
    #
    #c = AvroConsumer({"bootstrap.servers": BROKER_URL, "group.id": "0","auto.offset.reset":'earliest'}, schema_registry=schema_registry)
    c=Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0","auto.offset.reset":'earliest'})
    c.subscribe([topic_name])
    while True:
        try:
            msg = c.poll(1.0)
        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            raise SerializerError
        if msg is None:
            print("message is not received yet")
        elif msg.error():
            print("AvroConsumer error: {}".format(msg.error()))
            return
        else:
            key, value = msg.key(),msg.value()
            print(key, value)
        await asyncio.sleep(1.0)


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
       asyncio.run(produce_consume("call_details"))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    #t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t2


if __name__ == "__main__":
    main()


