import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from confluent_kafka import Consumer, Producer
import json
import datetime

from config import (
    setup_logger,
    get_consumer_config,
    get_producer_config,
    TOPIC_TRADES,
    TOPIC_ENRICHED_TRADES,
    CONSUMER_GROUP_VALIDATION,
    CONSUMER_ID_VALIDATION,
    PRODUCER_ID_ENRICHMENT
)

# Setup logger
logger = setup_logger("validation_enrichment", "validation_enrichment.log")

# Consumer configuration
consumer_conf = get_consumer_config(
    group_id=CONSUMER_GROUP_VALIDATION,
    client_id=CONSUMER_ID_VALIDATION,
    auto_offset_reset="earliest"
)

# Producer configuration
producer_conf = get_producer_config(client_id=PRODUCER_ID_ENRICHMENT)

# Create Consumer instance
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_TRADES])

# Create Producer instance
producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback to confirm delivery or error"""
    if err is not None:
        logger.error(f"Delivery failed: {err}")

def validate_and_enrich(trade):
    """Validate and enrich trade data"""
    # Basic validation - using correct field names
    if not trade.get("symbol") or trade.get("quantity", 0) <= 0 or trade.get("price", 0) <= 0:
        logger.warning(f"Invalid trade skipped: {trade}")
        return None

    # Enrichment
    trade["trade_value"] = trade["quantity"] * trade["price"]
    trade["validated_at"] = datetime.datetime.utcnow().isoformat()
    trade["counterparty"] = "Counterparty_XYZ"  # mock enrichment
    trade["side"] = trade.get("side", "BUY")  # default BUY if missing

    return trade

logger.info(f"Validation & Enrichment Consumer started [ID: {CONSUMER_ID_VALIDATION}]")
logger.info(f"Consuming from: {TOPIC_TRADES} -> Publishing to: {TOPIC_ENRICHED_TRADES}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        # Deserialize message
        trade = json.loads(msg.value().decode('utf-8'))
        logger.info(f"Received trade: {trade}")

        enriched_trade = validate_and_enrich(trade)
        if enriched_trade:
            # Send to enriched-trades topic
            producer.produce(
                topic=TOPIC_ENRICHED_TRADES,
                value=json.dumps(enriched_trade).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)  # Trigger delivery callbacks
            logger.info(f"Sent enriched trade: trade_id={enriched_trade['trade_id']}, symbol={enriched_trade['symbol']}")

except KeyboardInterrupt:
    logger.info("Stopping validation consumer...")
finally:
    producer.flush()
    consumer.close()
    logger.info("Consumer closed")