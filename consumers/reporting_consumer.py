from kafka import KafkaConsumer
import json
import csv
import pathlib
import datetime
from collections import defaultdict

# Paths
REPORTS_DIR = pathlib.Path(__file__).parent.parent / "reports"
REPORTS_DIR.mkdir(exist_ok=True)  # ensure reports folder exists

# Kafka setup
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "reports"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    group_id="reporting_group",
    enable_auto_commit=True
)

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
    print(f"💾 Saved report to: {filepath}")

def generate_console_report(report_data):
    """Generate formatted console output"""
    report_type = report_data.get("report_type")
    timestamp = report_data.get("timestamp", "")
    data = report_data.get("data", {})
    
    print(f"\n{'='*60}")
    print(f"📊 {report_type.upper()} REPORT - {timestamp[:19]}")
    print(f"{'='*60}")
    
    if report_type == "positions":
        print("📈 CURRENT POSITIONS:")
        for symbol, position in data.get("positions", {}).items():
            status = "LONG" if position > 0 else "SHORT" if position < 0 else "FLAT"
            print(f"  {symbol:>6}: {position:>8} shares ({status})")
        print(f"\n🔢 Total Symbols: {data.get('total_symbols', 0)}")
        print(f"⚖️  Net Position: {data.get('net_positions', 0):>8} shares")
    
    elif report_type == "pnl":
        print("💰 PROFIT & LOSS:")
        for symbol, symbol_pnl in data.get("pnl_by_symbol", {}).items():
            color = "🟢" if symbol_pnl >= 0 else "🔴"
            print(f"  {symbol:>6}: {color} ${symbol_pnl:>10,.2f}")
        
        total_pnl = data.get("total_pnl", 0)
        pnl_color = "🟢" if total_pnl >= 0 else "🔴"
        print(f"\n{pnl_color} TOTAL PnL: ${total_pnl:>10,.2f}")
        print(f"📊 Total Volume: ${data.get('total_volume', 0):>10,.2f}")
        print(f"📝 Trade Count: {data.get('trade_count', 0):>8}")
        print(f"📏 Avg Trade Size: ${data.get('avg_trade_size', 0):>8,.2f}")
    
    elif report_type == "trade_summary":
        side_emoji = "🟢" if data.get("side") == "BUY" else "🔴"
        print(f"🆔 Trade ID: {data.get('trade_id')}")
        print(f"📊 Symbol: {data.get('symbol')}")
        print(f"{side_emoji} Side: {data.get('side')} {data.get('quantity')} @ ${data.get('price')}")
        print(f"💵 Trade Value: ${data.get('trade_value', 0):,.2f}")
        print(f"📈 Current Position: {data.get('current_position', 0)} shares")
        print(f"💰 Symbol PnL: ${data.get('symbol_pnl', 0):,.2f}")

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

print("📋 Reporting Consumer started... Listening for reports on 'reports' topic.")
print(f"📁 Reports will be saved to: {REPORTS_DIR}")
print("💡 Waiting for reports...")

try:
    report_count = 0
    for msg in consumer:
        report_count += 1
        report_data = msg.value
        
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
            print(f"\n💾 Auto-saving reports after {report_count} messages...")
            save_daily_reports()
            print("✅ CSV files updated!")

except KeyboardInterrupt:
    print("\n🛑 Stopping reporting consumer...")
    print("💾 Saving final reports...")
    save_daily_reports()
    print("✅ All reports saved!")
finally:
    consumer.close()