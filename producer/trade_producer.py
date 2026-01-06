from confluent_kafka import Producer
import json
import time
import random

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Create Producer instance
producer = Producer(conf)

def delivery_report(err, msg):
    """Callback to confirm delivery or error"""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# List of symbols for demo
symbols = ["AAPL", "MSFT", "GOOG", "TSLA"]

# Produce 10 random trades
for i in range(250):
    trade = {
        "trade_id": i + 1,
        "symbol": random.choice(symbols),
        "quantity": random.randint(10, 500),  # Changed from "qty" to "quantity"
        "price": round(random.uniform(100, 500), 2),
        "side": random.choice(["BUY", "SELL"])
    }

    producer.produce(
        topic="trades",
        key=str(trade["trade_id"]),
        value=json.dumps(trade),
        callback=delivery_report
    )

    # Flush messages asynchronously
    producer.poll(0)

    time.sleep(1)

# Wait for all messages to be delivered
producer.flush()