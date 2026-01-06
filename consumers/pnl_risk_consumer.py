from kafka import KafkaConsumer, KafkaProducer
import sqlite3
import json
import pathlib
import datetime

# Paths
DB_PATH = pathlib.Path(__file__).parent.parent / "db" / "trades.db"
DB_PATH.parent.mkdir(exist_ok=True)  # ensure db folder exists

# Kafka setup
BOOTSTRAP_SERVERS = "localhost:9092"
CONSUME_TOPIC = "enriched-trades"
REPORT_TOPIC = "reports"

# Consumer for enriched trades
consumer = KafkaConsumer(
    CONSUME_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="pnl_risk_group",
    enable_auto_commit=True
)

# Producer for reports
report_producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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
print("✅ Database table created/recreated with correct schema")

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

print("🚀 PnL + Risk Consumer started... Listening for trades on 'enriched-trades'.")
print("📊 Publishing reports to 'reports' topic.")
print("💡 Processing and waiting for trades...")

try:
    message_count = 0
    for msg in consumer:
        message_count += 1
        trade = msg.value
        print(f"🔥 Received enriched trade #{message_count}: {trade}")

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

        print(f"📊 Positions: {positions}")
        print(f"💰 PnL: {pnl}")
        
        # Generate and send reports to Kafka
        # 1. Trade summary report (for each trade)
        trade_report = generate_trade_summary_report(trade)
        report_producer.send(REPORT_TOPIC, trade_report)
        
        # 2. Position report (every trade)
        position_report = generate_position_report()
        report_producer.send(REPORT_TOPIC, position_report)
        
        # 3. PnL report (every 3 trades or can be customized)
        if message_count % 3 == 0:
            pnl_report = generate_pnl_report()
            report_producer.send(REPORT_TOPIC, pnl_report)
            print("📈 Sent comprehensive PnL report to Kafka")
        
        # Flush reports
        report_producer.flush()
        print("📤 Reports sent to Kafka topic 'reports'")
        print("--------------------------------------------------")

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    report_producer.close()
    consumer.close()
    conn.close()