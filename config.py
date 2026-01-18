"""
Centralized configuration for the post-trade-engine.
All Kafka and application settings are defined here.
"""
import pathlib
import logging

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_DIR = pathlib.Path(__file__).parent
DB_PATH = BASE_DIR / "db" / "trades.db"
REPORTS_DIR = BASE_DIR / "reports"
LOGS_DIR = BASE_DIR / "logs"

# Ensure directories exist
DB_PATH.parent.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# =============================================================================
# KAFKA CONFIGURATION
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Topic names
TOPIC_TRADES = "trades"
TOPIC_ENRICHED_TRADES = "enriched-trades"
TOPIC_REPORTS = "reports"

# Consumer Group IDs
CONSUMER_GROUP_VALIDATION = "validation_group"
CONSUMER_GROUP_PNL_RISK = "pnl_risk_group"
CONSUMER_GROUP_REPORTING = "reporting_group"

# Consumer Client IDs (unique identifiers)
CONSUMER_ID_VALIDATION = "validation-consumer-01"
CONSUMER_ID_PNL_RISK = "pnl-risk-consumer-01"
CONSUMER_ID_REPORTING = "reporting-consumer-01"

# Producer Client IDs
PRODUCER_ID_TRADE = "trade-producer-01"
PRODUCER_ID_ENRICHMENT = "enrichment-producer-01"
PRODUCER_ID_PNL_RISK = "pnl-risk-producer-01"


# =============================================================================
# BENCHMARKING CONFIGURATION
# =============================================================================
BENCHMARK_DIR = BASE_DIR / "benchmarking"
BENCHMARK_RESULTS_DIR = BENCHMARK_DIR / "results"

# Ensure benchmarking directories exist
BENCHMARK_RESULTS_DIR.mkdir(exist_ok=True, parents=True)

# Benchmark defaults
BENCHMARK_PRODUCER_ID = "benchmark-producer-01"
DEFAULT_BENCHMARK_TPS = 5000
DEFAULT_BENCHMARK_DURATION = 60  # seconds

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Create and configure a logger instance.
    
    Args:
        name: Logger name (typically module or component name)
        log_file: Optional log file name (will be created in LOGS_DIR)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
    logger.addHandler(console_handler)
    
    # File handler (if log_file specified)
    if log_file:
        file_handler = logging.FileHandler(LOGS_DIR / log_file)
        file_handler.setLevel(LOG_LEVEL)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        logger.addHandler(file_handler)
    
    return logger


def get_consumer_config(group_id: str, client_id: str, auto_offset_reset: str = "latest") -> dict:
    """
    Generate Confluent Kafka consumer configuration.
    
    Args:
        group_id: Consumer group ID
        client_id: Unique client identifier
        auto_offset_reset: Where to start reading ('earliest' or 'latest')
    
    Returns:
        Dictionary with consumer configuration
    """
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'client.id': client_id,
        'auto.offset.reset': auto_offset_reset,
        'enable.auto.commit': True
    }


def get_producer_config(client_id: str, idempotent: bool = True) -> dict:
    """
    Generate Confluent Kafka producer configuration.
    
    Args:
        client_id: Unique client identifier
        idempotent: Enable idempotent producer (default True for exactly-once semantics)
    
    Returns:
        Dictionary with producer configuration
    """
    config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': client_id
    }
    
    if idempotent:
        # Idempotent producer settings for exactly-once semantics
        config.update({
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 5,
            'max.in.flight.requests.per.connection': 5
        })
    
    return config

