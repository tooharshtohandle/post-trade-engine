import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from confluent_kafka import Producer
import json
import time
import random

from config import (
    setup_logger,
    get_producer_config,
    TOPIC_TRADES,
    PRODUCER_ID_TRADE
)

# Setup logger
logger = setup_logger("trade_producer", "trade_producer.log")

# Producer configuration
producer_conf = get_producer_config(client_id=PRODUCER_ID_TRADE)

# Create Producer instance
producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback to confirm delivery or error"""
    if err is not None:
        logger.error(f"Delivery failed for record {msg.key()}: {err}")

# List of symbols for demo
symbols = ["AAPL", "MSFT", "GOOG", "TSLA"]

logger.info(f"Trade Producer started [ID: {PRODUCER_ID_TRADE}]")
logger.info(f"Publishing to: {TOPIC_TRADES}")

# Produce 250 random trades
for i in range(250):
    trade = {
        "trade_id": i + 1,
        "symbol": random.choice(symbols),
        "quantity": random.randint(10, 500),
        "price": round(random.uniform(100, 500), 2),
        "side": random.choice(["BUY", "SELL"])
    }

    producer.produce(
        topic=TOPIC_TRADES,
        key=str(trade["trade_id"]),
        value=json.dumps(trade),
        callback=delivery_report
    )
    
    logger.info(f"Produced trade: trade_id={trade['trade_id']}, symbol={trade['symbol']}, side={trade['side']}, qty={trade['quantity']}, price={trade['price']}")

    # Flush messages asynchronously
    producer.poll(0)

    time.sleep(1)

# Wait for all messages to be delivered
producer.flush()
logger.info("All trades produced successfully")