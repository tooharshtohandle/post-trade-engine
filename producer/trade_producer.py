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

# ============================================================================
# BATCH CONFIGURATION - Tune these for performance
# ============================================================================
BATCH_SIZE = 100  # Commit every N trades (increase for higher throughput)
REPORT_BATCH_SIZE = 50  # Send reports every N trades (reduce Kafka overhead)

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

# ============================================================================
# SQLite DB Setup with WAL Mode and Performance Optimizations
# ============================================================================
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Enable WAL mode for better concurrency and performance
logger.info("Enabling SQLite optimizations...")
cur.execute("PRAGMA journal_mode=WAL")
cur.execute("PRAGMA synchronous=NORMAL")  # Trade some durability for speed
cur.execute("PRAGMA cache_size=-64000")  # 64MB cache
cur.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
cur.execute("PRAGMA mmap_size=268435456")  # 256MB memory-mapped I/O

# Log the active mode
wal_mode = cur.execute("PRAGMA journal_mode").fetchone()[0]
sync_mode = cur.execute("PRAGMA synchronous").fetchone()[0]
logger.info(f"SQLite journal_mode: {wal_mode}")
logger.info(f"SQLite synchronous: {sync_mode}")

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

# Create index for faster symbol queries (optional, adds slight overhead on insert)
# cur.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)")

conn.commit()
logger.info("Database table created/recreated with correct schema and optimizations enabled")

# ============================================================================
# Running positions and PnL
# ============================================================================
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
logger.info(f"Batch Configuration: DB Batch={BATCH_SIZE}, Report Batch={REPORT_BATCH_SIZE}")

# ============================================================================
# Batching Variables
# ============================================================================
trade_batch = []  # Accumulate trades for batch insert
message_count = 0
last_commit_count = 0

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            # No message received, check if we have pending batch
            if trade_batch:
                # Commit any remaining trades in batch
                cur.executemany("""
                    INSERT OR IGNORE INTO trades 
                    (trade_id, symbol, quantity, price, side, trade_value, validated_at, counterparty)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, trade_batch)
                conn.commit()
                logger.info(f"Committed final batch of {len(trade_batch)} trades")
                trade_batch = []
            continue
        
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        message_count += 1
        
        # Deserialize message
        trade = json.loads(msg.value().decode('utf-8'))
        logger.debug(f"Received enriched trade #{message_count}: trade_id={trade.get('trade_id')}, symbol={trade.get('symbol')}")

        # Add trade to batch (as tuple for executemany)
        trade_tuple = (
            trade.get("trade_id"),
            trade.get("symbol"),
            trade.get("quantity"),
            trade.get("price"),
            trade.get("side", "BUY"),
            trade.get("trade_value"),
            trade.get("validated_at"),
            trade.get("counterparty")
        )
        trade_batch.append(trade_tuple)

        # Update positions and PnL (in memory, immediately)
        symbol = trade["symbol"]
        qty = trade["quantity"] if trade["side"] == "BUY" else -trade["quantity"]
        positions[symbol] = positions.get(symbol, 0) + qty
        pnl[symbol] = pnl.get(symbol, 0) + (qty * trade["price"])
        
        # Update global metrics
        total_volume += trade["trade_value"]
        trade_count += 1

        # Batch commit when batch size is reached
        if len(trade_batch) >= BATCH_SIZE:
            # Batch insert all trades at once
            cur.executemany("""
                INSERT OR IGNORE INTO trades 
                (trade_id, symbol, quantity, price, side, trade_value, validated_at, counterparty)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, trade_batch)
            conn.commit()
            
            logger.info(f"✅ Batch committed: {len(trade_batch)} trades | Total processed: {message_count}")
            last_commit_count = message_count
            trade_batch = []  # Clear batch
        
        # Generate and send reports (less frequently to reduce overhead)
        if message_count % REPORT_BATCH_SIZE == 0:
            # Send comprehensive reports every N trades
            
            # Position report
            position_report = generate_position_report()
            report_producer.produce(
                topic=TOPIC_REPORTS,
                value=json.dumps(position_report).encode('utf-8'),
                callback=delivery_report
            )
            
            # PnL report
            pnl_report = generate_pnl_report()
            report_producer.produce(
                topic=TOPIC_REPORTS,
                value=json.dumps(pnl_report).encode('utf-8'),
                callback=delivery_report
            )
            
            # Trigger delivery callbacks
            report_producer.poll(0)
            logger.info(f"📊 Sent position & PnL reports (trade #{message_count})")

except KeyboardInterrupt:
    logger.info("Stopping consumer...")
finally:
    # Commit any remaining trades in batch
    if trade_batch:
        cur.executemany("""
            INSERT OR IGNORE INTO trades 
            (trade_id, symbol, quantity, price, side, trade_value, validated_at, counterparty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, trade_batch)
        conn.commit()
        logger.info(f"Final commit: {len(trade_batch)} trades")
    
    report_producer.flush()
    consumer.close()
    conn.close()
    logger.info(f"Consumer closed - Total trades processed: {message_count}")