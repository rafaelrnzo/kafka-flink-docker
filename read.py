from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
import json


def process_message(message):
    # Parse the JSON string into a Python dictionary
    try:
        transaction = json.loads(message)
        print(f"Transaction ID: {transaction['transactionId']}, Total Amount: {transaction['totalAmount']}")
    except json.JSONDecodeError:
        print(f"Invalid message received: {message}")


def main():
    # Set up the Flink streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set up Kafka consumer properties
    kafka_properties = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'flink-consumer-group',  # Consumer group ID
        'auto.offset.reset': 'earliest'  # Start reading from the earliest available message
    }

    # Define the Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics='financial_transactions',
        deserialization_schema=SimpleStringSchema(),  # Deserialize messages as plain strings
        properties=kafka_properties
    )

    # Add the Kafka source to the Flink job
    stream = env.add_source(kafka_consumer, type_info=Types.STRING())

    # Process the data stream
    stream.map(lambda message: process_message(message), output_type=Types.VOID())

    # Execute the Flink job
    env.execute("Kafka Consumer Job")


if __name__ == '__main__':
    main()
