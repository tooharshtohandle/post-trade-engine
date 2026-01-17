import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from confluent_kafka import Consumer
import json
import csv
import datetime
from collections import defaultdict

from config import (
    setup_logger,
    get_consumer_config,
    REPORTS_DIR,
    TOPIC_REPORTS,
    CONSUMER_GROUP_REPORTING,
    CONSUMER_ID_REPORTING
)

# Setup logger
logger = setup_logger("reporting", "reporting.log")

# Consumer configuration
consumer_conf = get_consumer_config(
    group_id=CONSUMER_GROUP_REPORTING,
    client_id=CONSUMER_ID_REPORTING,
    auto_offset_reset="latest"
)

# Create Consumer instance
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC_REPORTS])

# Report storage for aggregation
daily_summary = defaultdict(list)
position_history = []
pnl_history = []

def save_to_csv(filename, data, headers):
    """Save data to CSV file"""
    filepath = REPORTS_DIR / filename
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Saved report to: {filepath}")

def generate_console_report(report_data):
    """Generate formatted console output"""
    report_type = report_data.get("report_type")
    timestamp = report_data.get("timestamp", "")
    data = report_data.get("data", {})
    
    logger.info(f"{'='*60}")
    logger.info(f"{report_type.upper()} REPORT - {timestamp[:19]}")
    logger.info(f"{'='*60}")
    
    if report_type == "positions":
        logger.info("CURRENT POSITIONS:")
        for symbol, position in data.get("positions", {}).items():
            status = "LONG" if position > 0 else "SHORT" if position < 0 else "FLAT"
            logger.info(f"  {symbol:>6}: {position:>8} shares ({status})")
        logger.info(f"Total Symbols: {data.get('total_symbols', 0)}")
        logger.info(f"Net Position: {data.get('net_positions', 0):>8} shares")
    
    elif report_type == "pnl":
        logger.info("PROFIT & LOSS:")
        for symbol, symbol_pnl in data.get("pnl_by_symbol", {}).items():
            status = "GAIN" if symbol_pnl >= 0 else "LOSS"
            logger.info(f"  {symbol:>6}: ${symbol_pnl:>10,.2f} ({status})")
        
        total_pnl = data.get("total_pnl", 0)
        pnl_status = "GAIN" if total_pnl >= 0 else "LOSS"
        logger.info(f"TOTAL PnL: ${total_pnl:>10,.2f} ({pnl_status})")
        logger.info(f"Total Volume: ${data.get('total_volume', 0):>10,.2f}")
        logger.info(f"Trade Count: {data.get('trade_count', 0):>8}")
        logger.info(f"Avg Trade Size: ${data.get('avg_trade_size', 0):>8,.2f}")
    
    elif report_type == "trade_summary":
        logger.info(f"Trade ID: {data.get('trade_id')}")
        logger.info(f"Symbol: {data.get('symbol')}")
        logger.info(f"Side: {data.get('side')} {data.get('quantity')} @ ${data.get('price')}")
        logger.info(f"Trade Value: ${data.get('trade_value', 0):,.2f}")
        logger.info(f"Current Position: {data.get('current_position', 0)} shares")
        logger.info(f"Symbol PnL: ${data.get('symbol_pnl', 0):,.2f}")

def save_daily_reports():
    """Save accumulated data to daily CSV files"""
    today = datetime.date.today().strftime("%Y%m%d")
    
    # Save positions history
    if position_history:
        pos_filename = f"positions_{today}.csv"
        headers = ["timestamp", "symbol", "position", "total_symbols", "net_positions"]
        pos_data = []
        for report in position_history:
            timestamp = report["timestamp"]
            for symbol, position in report["data"]["positions"].items():
                pos_data.append({
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "position": position,
                    "total_symbols": report["data"]["total_symbols"],
                    "net_positions": report["data"]["net_positions"]
                })
        save_to_csv(pos_filename, pos_data, headers)
    
    # Save PnL history
    if pnl_history:
        pnl_filename = f"pnl_summary_{today}.csv"
        headers = ["timestamp", "symbol", "pnl", "total_pnl", "total_volume", "trade_count", "avg_trade_size"]
        pnl_data = []
        for report in pnl_history:
            timestamp = report["timestamp"]
            for symbol, symbol_pnl in report["data"]["pnl_by_symbol"].items():
                pnl_data.append({
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "pnl": symbol_pnl,
                    "total_pnl": report["data"]["total_pnl"],
                    "total_volume": report["data"]["total_volume"],
                    "trade_count": report["data"]["trade_count"],
                    "avg_trade_size": report["data"]["avg_trade_size"]
                })
        save_to_csv(pnl_filename, pnl_data, headers)

logger.info(f"Reporting Consumer started [ID: {CONSUMER_ID_REPORTING}]")
logger.info(f"Consuming from: {TOPIC_REPORTS}")
logger.info(f"Reports will be saved to: {REPORTS_DIR}")

try:
    report_count = 0
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        
        report_count += 1
        # Deserialize message
        report_data = json.loads(msg.value().decode('utf-8'))
        
        # Display console report
        generate_console_report(report_data)
        
        # Store for CSV generation
        report_type = report_data.get("report_type")
        if report_type == "positions":
            position_history.append(report_data)
        elif report_type == "pnl":
            pnl_history.append(report_data)
        
        # Auto-save CSV files every 5 reports
        if report_count % 5 == 0:
            logger.info(f"Auto-saving reports after {report_count} messages...")
            save_daily_reports()
            logger.info("CSV files updated!")

except KeyboardInterrupt:
    logger.info("Stopping reporting consumer...")
    logger.info("Saving final reports...")
    save_daily_reports()
    logger.info("All reports saved!")
finally:
    consumer.close()
    logger.info("Consumer closed")