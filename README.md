# Real-Time Trade Processing Pipeline

This project implements a real-time trade processing pipeline using Kafka and Python. The system is designed to handle a continuous stream of trades, validate them, enrich them with additional data, calculate profit and loss (PnL) and risk metrics, and generate reports.

## Architecture

The pipeline consists of the following components:

- **Trade Producer:** A Python script that generates random trade data and sends it to a Kafka topic named `trades`.
- **Validation and Enrichment Consumer:** This consumer reads from the `trades` topic, validates the trades, enriches them with additional information (e.g., trade value, counterparty), and sends the enriched trades to the `enriched-trades` topic.
- **PnL and Risk Consumer:** This consumer processes the enriched trades, calculates PnL and risk metrics, and stores the trades in a SQLite database. It also produces reports to the `reports` topic.
- **Reporting Consumer:** This consumer reads from the `reports` topic, displays the reports in the console, and saves them to CSV files.

## Components

### Producer

- `producer/trade_producer.py`: This script generates random trades and sends them to the `trades` Kafka topic.

### Consumers

- `consumers/validation_enrichment_consumer.py`: Consumes raw trades, validates them, and enriches them with additional data.
- `consumers/pnl_risk_consumer.py`: Consumes enriched trades, calculates PnL and risk, and stores trades in a database.
- `consumers/reporting_consumer.py`: Consumes reports and outputs them to the console and CSV files.

### Database

- `db/trades.db`: A SQLite database used to store the processed trades.

### Reports

- `reports/`: This directory contains the generated CSV reports.

## Getting Started

### Prerequisites

- Python 3.x
- Kafka
- The Python dependencies listed in `requirements.txt`.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    ```
2.  **Install the Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Start Kafka:**
    Follow the instructions in the official Kafka documentation to start a Kafka broker.

### Running the System

1.  **Start the consumers:**
    Open a terminal and run the following command to start all the consumers:
    ```bash
    python consumers/validation_enrichment_consumer.py &
    python consumers/pnl_risk_consumer.py &
    python consumers/reporting_consumer.py &
    ```
2.  **Start the producer:**
    Open another terminal and run the following command to start the trade producer:
    ```bash
    python producer/trade_producer.py
    ```

This will start the entire trade processing pipeline. You will see the trades being processed in real-time in the consumer terminals, and the reports will be generated in the `reports` directory.
