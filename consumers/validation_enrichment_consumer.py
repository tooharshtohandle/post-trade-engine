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

# ============================================================================
# BATCH CONFIGURATION
# ============================================================================
PRODUCER_BATCH_SIZE = 50  # Send enriched trades in batches
PRODUCER_FLUSH_INTERVAL = 0.1  # Flush every 100ms if batch not full

# Consumer configuration with batching optimizations
consumer_conf = get_consumer_config(
    group_id=CONSUMER_GROUP_VALIDATION,
    client_id=CONSUMER_ID_VALIDATION,
    auto_offset_reset="earliest"
)
# Optimize consumer to fetch multiple messages at once
consumer_conf.update({
    'fetch.min.bytes': 1024,  # Wait for at least 1KB of data
    'fetch.wait.max.ms': 100,  # Or wait max 100ms
})

# Producer configuration with batching optimizations
producer_conf = get_producer_config(client_id=PRODUCER_ID_ENRICHMENT)
# Optimize producer for batching
producer_conf.update({
    'linger.ms': 10,  # Wait 10ms to batch messages
    'batch.size': 32768,  # 32KB batch size
    'compression.type': 'lz4',  # Fast compression
})

# Create Consumer instance
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_TRADES])

# Create Producer instance
producer = Producer(producer_conf)

# Statistics
trades_processed = 0
trades_invalid = 0
last_flush_time = datetime.datetime.utcnow()

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
logger.info(f"Producer Batch Config: batch_size={PRODUCER_BATCH_SIZE}, flush_interval={PRODUCER_FLUSH_INTERVAL}s")

# ============================================================================
# Batching Variables
# ============================================================================
pending_messages = 0  # Track messages waiting to be flushed

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            # No message - check if we should flush pending messages
            current_time = datetime.datetime.utcnow()
            time_since_flush = (current_time - last_flush_time).total_seconds()
            
            if pending_messages > 0 and time_since_flush >= PRODUCER_FLUSH_INTERVAL:
                producer.flush()
                logger.debug(f"Flushed {pending_messages} pending messages (timeout)")
                pending_messages = 0
                last_flush_time = current_time
            continue
        
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        # Deserialize message
        trade = json.loads(msg.value().decode('utf-8'))
        logger.debug(f"Received trade: {trade}")

        enriched_trade = validate_and_enrich(trade)
        if enriched_trade:
            # Send to enriched-trades topic (buffered, not flushed immediately)
            producer.produce(
                topic=TOPIC_ENRICHED_TRADES,
                value=json.dumps(enriched_trade).encode('utf-8'),
                callback=delivery_report
            )
            
            trades_processed += 1
            pending_messages += 1
            
            # Poll to trigger callbacks (non-blocking)
            producer.poll(0)
            
            logger.debug(f"Queued enriched trade: trade_id={enriched_trade['trade_id']}, symbol={enriched_trade['symbol']}")
            
            # Flush when batch size is reached
            if pending_messages >= PRODUCER_BATCH_SIZE:
                producer.flush()
                logger.info(f"✅ Flushed batch: {pending_messages} trades | Total processed: {trades_processed}")
                pending_messages = 0
                last_flush_time = datetime.datetime.utcnow()
        else:
            trades_invalid += 1
        
        # Periodic logging
        if trades_processed % 100 == 0 and trades_processed > 0:
            logger.info(f"Progress: {trades_processed} trades processed, {trades_invalid} invalid")

except KeyboardInterrupt:
    logger.info("Stopping validation consumer...")
finally:
    # Flush any remaining messages
    if pending_messages > 0:
        logger.info(f"Flushing {pending_messages} remaining messages...")
        producer.flush()
    
    consumer.close()
    logger.info(f"Consumer closed - Total processed: {trades_processed}, Invalid: {trades_invalid}")