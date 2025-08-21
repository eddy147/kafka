import sys
import argparse
import time
from kafka import KafkaProducer

def send_kafka_message(producer, topic, hex_string):
    """Decodes a single hex string and sends it to Kafka."""
    # Skip empty or whitespace-only lines
    if not hex_string.strip():
        return False

    try:
        # Decode the hex string to bytes
        payload = bytes.fromhex(hex_string.replace(' ', ''))
        print(f"Sending payload: {payload}")

        # Send the message and wait for confirmation
        future = producer.send(topic, payload)
        future.get(timeout=10)
        print(" -> Sent successfully.")
        return True

    except Exception as e:
        print(f" -> FAILED to send: {e}", file=sys.stderr)
        return False

def main():
    """Main function to parse arguments and run the producer."""
    parser = argparse.ArgumentParser(
        description="Standalone Kafka Producer. Reads a file where each line is a space-separated hex string and sends each line as a separate message."
    )
    parser.add_argument("filename", help="Path to the file containing hex data.")
    args = parser.parse_args()

    bootstrap_servers = 'localhost:9092'
    topic = 'aws-connect'
    producer = None

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            api_version_auto_timeout_ms=5000
        )
        print(f"--- Producer Script ---")
        print(f"Connecting to: {bootstrap_servers}")
        print(f"Target topic:  {topic}")
        print(f"Reading from file: {args.filename}\n")

        with open(args.filename, 'r') as f:
            for i, line in enumerate(f):
                print(f"Processing line {i+1}...")
                send_kafka_message(producer, topic, line)
                # Add a small delay to avoid overwhelming the consumer
                time.sleep(0.2)

    except FileNotFoundError:
        print(f"Error: File not found at '{args.filename}'", file=sys.stderr)
    except Exception as e:
        print(f"\nAn error occurred: {e}", file=sys.stderr)
    finally:
        if producer:
            print("\nFlushing and closing producer...")
            producer.flush()
            producer.close()
            print("--- End of Script ---")

if __name__ == "__main__":
    main()