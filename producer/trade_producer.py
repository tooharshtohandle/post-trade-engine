from kafka import KafkaProducer
import json
import time
import random

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

def on_send_success(record_metadata):
    print(f"✅ Record successfully produced to {record_metadata.topic} partition {record_metadata.partition} at offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"❌ Delivery failed: {excp}")

# List of symbols for demo
symbols = ["AAPL", "MSFT", "GOOG", "TSLA"]

# Produce 250 random trades
print("🚀 Starting trade producer...")
for i in range(250):
    trade = {
        "trade_id": i + 1,
        "symbol": random.choice(symbols),
        "quantity": random.randint(10, 500),
        "price": round(random.uniform(100, 500), 2),
        "side": random.choice(["BUY", "SELL"])
    }

    # Send message
    future = producer.send(
        "trades",
        key=trade["trade_id"],
        value=trade
    )
    # Add callbacks for success and failure
    future.add_callback(on_send_success)
    future.add_errback(on_send_error)

    print(f"🔥 Sent trade: {trade}")
    time.sleep(1)

# Wait for all messages to be delivered
print("Flushing messages...")
producer.flush()
print("All messages flushed.")
producer.close()
print("Producer closed.")
