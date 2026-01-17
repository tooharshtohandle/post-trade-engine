import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from confluent_kafka import Consumer, Producer
import sqlite3
import json
import datetime

from config import (
    setup_logger,
    get_consumer_config,
    get_producer_config,
    DB_PATH,
    TOPIC_ENRICHED_TRADES,
    TOPIC_REPORTS,
    CONSUMER_GROUP_PNL_RISK,
    CONSUMER_ID_PNL_RISK,
    PRODUCER_ID_PNL_RISK
)

# Setup logger
logger = setup_logger("pnl_risk", "pnl_risk.log")

# Consumer configuration
consumer_conf = get_consumer_config(
    group_id=CONSUMER_GROUP_PNL_RISK,
    client_id=CONSUMER_ID_PNL_RISK,
    auto_offset_reset="latest"
)

# Producer configuration
producer_conf = get_producer_config(client_id=PRODUCER_ID_PNL_RISK)

# Create Consumer instance
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_ENRICHED_TRADES])

# Create Producer instance for reports
report_producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback to confirm delivery or error"""
    if err is not None:
        logger.error(f"Report delivery failed: {err}")

# SQLite DB setup
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Drop the existing table if it exists (to handle schema changes)
cur.execute("DROP TABLE IF EXISTS trades")

# Create the table with correct schema
cur.execute("""
CREATE TABLE trades (
    trade_id INTEGER PRIMARY KEY,
    symbol TEXT,
    quantity INTEGER,
    price REAL,
    side TEXT,
    trade_value REAL,
    validated_at TEXT,
    counterparty TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")
conn.commit()
logger.info("Database table created/recreated with correct schema")

# Running positions and PnL
positions = {}
pnl = {}
total_volume = 0
trade_count = 0

def generate_position_report():
    """Generate current positions report"""
    return {
        "report_type": "positions",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "data": {
            "positions": positions.copy(),
            "total_symbols": len(positions),
            "net_positions": sum(positions.values())
        }
    }

def generate_pnl_report():
    """Generate current PnL report"""
    total_pnl = sum(pnl.values())
    return {
        "report_type": "pnl",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "data": {
            "pnl_by_symbol": pnl.copy(),
            "total_pnl": total_pnl,
            "total_volume": total_volume,
            "trade_count": trade_count,
            "avg_trade_size": total_volume / trade_count if trade_count > 0 else 0
        }
    }

def generate_trade_summary_report(trade):
    """Generate individual trade summary"""
    return {
        "report_type": "trade_summary",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "data": {
            "trade_id": trade["trade_id"],
            "symbol": trade["symbol"],
            "side": trade["side"],
            "quantity": trade["quantity"],
            "price": trade["price"],
            "trade_value": trade["trade_value"],
            "current_position": positions.get(trade["symbol"], 0),
            "symbol_pnl": pnl.get(trade["symbol"], 0)
        }
    }

logger.info(f"PnL + Risk Consumer started [ID: {CONSUMER_ID_PNL_RISK}]")
logger.info(f"Consuming from: {TOPIC_ENRICHED_TRADES} -> Publishing to: {TOPIC_REPORTS}")

try:
    message_count = 0
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        message_count += 1
        # Deserialize message
        trade = json.loads(msg.value().decode('utf-8'))
        logger.info(f"Received enriched trade #{message_count}: trade_id={trade.get('trade_id')}, symbol={trade.get('symbol')}")

        # Insert trade into DB
        cur.execute("""
            INSERT OR IGNORE INTO trades (trade_id, symbol, quantity, price, side, trade_value, validated_at, counterparty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade.get("trade_id"),
            trade.get("symbol"),
            trade.get("quantity"),
            trade.get("price"),
            trade.get("side", "BUY"),
            trade.get("trade_value"),
            trade.get("validated_at"),
            trade.get("counterparty")
        ))
        conn.commit()

        # Update positions and PnL
        symbol = trade["symbol"]
        qty = trade["quantity"] if trade["side"] == "BUY" else -trade["quantity"]
        positions[symbol] = positions.get(symbol, 0) + qty
        pnl[symbol] = pnl.get(symbol, 0) + (qty * trade["price"])
        
        # Update global metrics
        total_volume += trade["trade_value"]
        trade_count += 1
        
        
        # Generate and send reports to Kafka
        # 1. Trade summary report (for each trade)
        trade_report = generate_trade_summary_report(trade)
        report_producer.produce(
            topic=TOPIC_REPORTS,
            value=json.dumps(trade_report).encode('utf-8'),
            callback=delivery_report
        )
        
        # 2. Position report (every trade)
        position_report = generate_position_report()
        report_producer.produce(
            topic=TOPIC_REPORTS,
            value=json.dumps(position_report).encode('utf-8'),
            callback=delivery_report
        )
        
        # 3. PnL report (every 3 trades or can be customized)
        if message_count % 3 == 0:
            pnl_report = generate_pnl_report()
            report_producer.produce(
                topic=TOPIC_REPORTS,
                value=json.dumps(pnl_report).encode('utf-8'),
                callback=delivery_report
            )
            logger.info("Sent comprehensive PnL report to Kafka")
        
        # Trigger delivery callbacks and flush reports
        report_producer.poll(0)
        report_producer.flush()
        logger.info(f"Reports sent to Kafka topic '{TOPIC_REPORTS}'")

except KeyboardInterrupt:
    logger.info("Stopping consumer...")
finally:
    report_producer.flush()
    consumer.close()
    conn.close()
    logger.info("Consumer closed")