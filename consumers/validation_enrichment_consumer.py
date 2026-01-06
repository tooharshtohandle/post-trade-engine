from kafka import KafkaConsumer, KafkaProducer
import json
import datetime

# Simplified Kafka config - using direct connection
BOOTSTRAP_SERVERS = "localhost:9092"

# Consumer listens to 'trades'
consumer = KafkaConsumer(
    "trades",
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="validation_group"  # Added consumer group
)

# Producer publishes to 'enriched-trades'
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def validate_and_enrich(trade):
    # Basic validation - using correct field names
    if not trade.get("symbol") or trade.get("quantity", 0) <= 0 or trade.get("price", 0) <= 0:
        print(f"❌ Invalid trade skipped: {trade}")
        return None

    # Enrichment
    trade["trade_value"] = trade["quantity"] * trade["price"]
    trade["validated_at"] = datetime.datetime.utcnow().isoformat()
    trade["counterparty"] = "Counterparty_XYZ"  # mock enrichment
    trade["side"] = trade.get("side", "BUY")  # default BUY if missing

    return trade

print("🚀 Validation & Enrichment Consumer started...")

try:
    for message in consumer:
        trade = message.value
        print(f"🔥 Received trade: {trade}")

        enriched_trade = validate_and_enrich(trade)
        if enriched_trade:
            # Send to enriched-trades topic
            future = producer.send("enriched-trades", enriched_trade)
            producer.flush()  # Ensure message is sent immediately
            print(f"✅ Sent enriched trade: {enriched_trade}")

except KeyboardInterrupt:
    print("Stopping validation consumer...")
finally:
    producer.close()
    consumer.close()