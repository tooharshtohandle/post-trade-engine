# Post-Trade Engine 📈

A real-time post-trade processing system built with **Apache Kafka** and **Python**. This engine handles trade validation, enrichment, PnL calculation, risk monitoring, and automated reporting.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Confluent-orange.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│  Trade Producer │────▶│  Validation & Enrichment│────▶│  PnL & Risk     │────▶│  Reporting       │
│                 │     │  Consumer               │     │  Consumer       │     │  Consumer        │
└─────────────────┘     └─────────────────────────┘     └─────────────────┘     └──────────────────┘
        │                         │                             │                        │
        ▼                         ▼                             ▼                        ▼
   [trades]              [enriched-trades]                 [reports]              CSV Reports
    topic                     topic                         topic                   & Logs
```

## ✨ Features

- **Trade Validation** - Validates incoming trades for required fields and data integrity
- **Trade Enrichment** - Adds computed fields (trade value, timestamps, counterparty info)
- **Real-time PnL** - Calculates profit & loss per symbol and aggregate
- **Position Tracking** - Maintains running positions (long/short) per symbol
- **Risk Metrics** - Tracks volume, trade count, and average trade size
- **Automated Reporting** - Generates CSV reports for positions and PnL
- **Idempotent Producers** - Exactly-once message delivery semantics
- **Centralized Configuration** - All settings in one place (`config.py`)
- **Structured Logging** - Consistent logging with file and console outputs

## 📁 Project Structure

```
post-trade-engine/
├── config.py                 # Centralized configuration
├── requirements.txt          # Python dependencies
├── start_system.bat          # Windows batch script to start all consumers
├── README.md
│
├── producer/
│   └── trade_producer.py     # Generates sample trades
│
├── consumers/
│   ├── validation_enrichment_consumer.py   # Validates & enriches trades
│   ├── pnl_risk_consumer.py                # Calculates PnL & risk metrics
│   └── reporting_consumer.py               # Generates reports & CSV files
│
├── db/
│   └── trades.db             # SQLite database for trade storage
│
├── reports/                  # Generated CSV reports
│   ├── positions_YYYYMMDD.csv
│   └── pnl_summary_YYYYMMDD.csv
│
├── logs/                     # Application logs
│   ├── trade_producer.log
│   ├── validation_enrichment.log
│   ├── pnl_risk.log
│   └── reporting.log
│
└── config/
    └── kafka_config.json     # Legacy config (now using config.py)
```

## 🚀 Getting Started

### Prerequisites

- **Python 3.8+**
- **Apache Kafka** (with KRaft or Zookeeper)
- **Windows OS** (for batch scripts) or adapt commands for Linux/macOS

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/post-trade-engine.git
   cd post-trade-engine
   ```

2. **Create virtual environment** (recommended)
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   # source venv/bin/activate  # Linux/macOS
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Running the System

#### Step 1: Start Kafka Broker

```bash
# Navigate to your Kafka installation
cd C:/kafka/kafka

# Start the broker (KRaft mode)
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```

#### Step 2: Start Consumers

**Option A:** Use the batch script (Windows)
```bash
# Double-click start_system.bat
# OR run from terminal:
start_system.bat
```

**Option B:** Start manually in separate terminals
```bash
# Terminal 1 - Validation & Enrichment
python consumers/validation_enrichment_consumer.py

# Terminal 2 - PnL & Risk
python consumers/pnl_risk_consumer.py

# Terminal 3 - Reporting
python consumers/reporting_consumer.py
```

#### Step 3: Start Producer

```bash
python producer/trade_producer.py
```

## ⚙️ Configuration

All configuration is centralized in `config.py`:

| Setting | Description | Default |
|---------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9092` |
| `TOPIC_TRADES` | Raw trades topic | `trades` |
| `TOPIC_ENRICHED_TRADES` | Enriched trades topic | `enriched-trades` |
| `TOPIC_REPORTS` | Reports topic | `reports` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Consumer IDs

| Consumer | Client ID |
|----------|-----------|
| Validation | `validation-consumer-01` |
| PnL & Risk | `pnl-risk-consumer-01` |
| Reporting | `reporting-consumer-01` |

### Producer Settings

- **Idempotent**: `enable.idempotence = True`
- **Acknowledgments**: `acks = all`
- **Retries**: `5`

## 📊 Data Flow

### Trade Message Format

```json
{
  "trade_id": 1,
  "symbol": "AAPL",
  "quantity": 100,
  "price": 150.50,
  "side": "BUY"
}
```

### Enriched Trade Format

```json
{
  "trade_id": 1,
  "symbol": "AAPL",
  "quantity": 100,
  "price": 150.50,
  "side": "BUY",
  "trade_value": 15050.00,
  "validated_at": "2026-01-17T16:00:00.000Z",
  "counterparty": "Counterparty_XYZ"
}
```

### Report Types

- **trade_summary** - Individual trade details with current position
- **positions** - Current positions per symbol (long/short/flat)
- **pnl** - Profit & loss summary with volume metrics

## 📝 Logs

Logs are stored in the `logs/` directory with the format:
```
2026-01-17 21:30:00 | component_name | INFO | message
```

## 🛠️ Development

### Adding a New Consumer

1. Create a new file in `consumers/`
2. Import configuration from `config.py`
3. Use `setup_logger()` for consistent logging
4. Use `get_consumer_config()` for Kafka configuration

```python
from config import setup_logger, get_consumer_config, TOPIC_NAME

logger = setup_logger("my_consumer", "my_consumer.log")
consumer_conf = get_consumer_config(
    group_id="my_group",
    client_id="my-consumer-01"
)
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

**Built with ❤️ using Apache Kafka and Python**
