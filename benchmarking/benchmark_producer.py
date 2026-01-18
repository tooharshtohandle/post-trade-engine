"""
Benchmark Producer - High-speed trade generator for performance testing.

This producer generates trades as fast as possible to benchmark the system's
throughput. It tracks all sent trades and saves metadata for verification.

Usage:
    python benchmark_producer.py --tps 5000 --duration 60
    python benchmark_producer.py --total 100000
"""

import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from confluent_kafka import Producer
import json
import time
import random
import argparse
from datetime import datetime

from config import (
    setup_logger,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TRADES,
    BENCHMARK_RESULTS_DIR
)

# Setup logger
logger = setup_logger("benchmark_producer", "benchmark_producer.log")

# Symbols for random trade generation
SYMBOLS = ["AAPL", "MSFT", "GOOG", "TSLA", "AMZN", "META", "NVDA", "JPM"]

class BenchmarkProducer:
    def __init__(self, target_tps=None, duration=None, total_trades=None):
        """
        Initialize benchmark producer.
        
        Args:
            target_tps: Target trades per second (if None, go as fast as possible)
            duration: Duration in seconds (mutually exclusive with total_trades)
            total_trades: Total number of trades to send (mutually exclusive with duration)
        """
        self.target_tps = target_tps
        self.duration = duration
        self.total_trades = total_trades
        
        # Producer configuration - optimized for throughput
        self.producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'benchmark-producer-01',
            'enable.idempotence': True,
            'acks': 'all',
            'linger.ms': 10,  # Wait 10ms to batch messages (increases throughput)
            'batch.size': 32768,  # 32KB batch size
            'compression.type': 'lz4',  # Fast compression
            'queue.buffering.max.messages': 100000,  # Allow large queue
        }
        
        self.producer = Producer(self.producer_conf)
        
        # Tracking
        self.trades_sent = 0
        self.trades_acknowledged = 0
        self.failed_trades = 0
        self.sent_trade_ids = []
        self.start_time = None
        self.end_time = None
        
    def delivery_report(self, err, msg):
        """Callback for message delivery confirmation"""
        if err is not None:
            self.failed_trades += 1
            logger.error(f"Delivery failed for trade {msg.key()}: {err}")
        else:
            self.trades_acknowledged += 1
    
    def generate_trade(self, trade_id):
        """Generate a random trade with sequence number"""
        return {
            "trade_id": trade_id,
            "sequence_number": trade_id,  # For verification
            "symbol": random.choice(SYMBOLS),
            "quantity": random.randint(10, 1000),
            "price": round(random.uniform(50, 500), 2),
            "side": random.choice(["BUY", "SELL"]),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def run(self):
        """Execute the benchmark"""
        logger.info("=" * 60)
        logger.info("BENCHMARK PRODUCER STARTING")
        logger.info("=" * 60)
        
        if self.total_trades:
            logger.info(f"Mode: Total Trades = {self.total_trades:,}")
        elif self.duration:
            logger.info(f"Mode: Duration = {self.duration} seconds")
            if self.target_tps:
                logger.info(f"Target TPS: {self.target_tps:,}")
                self.total_trades = self.target_tps * self.duration
            else:
                logger.info("Target TPS: MAXIMUM (no throttling)")
        
        logger.info(f"Kafka Topic: {TOPIC_TRADES}")
        logger.info("=" * 60)
        
        self.start_time = time.time()
        trade_id = 1
        last_log_time = self.start_time
        
        # Calculate sleep time if target TPS is set
        sleep_time = (1.0 / self.target_tps) if self.target_tps else 0
        
        try:
            while True:
                # Check stopping conditions
                if self.total_trades and trade_id > self.total_trades:
                    break
                if self.duration and (time.time() - self.start_time) >= self.duration:
                    break
                
                # Generate and send trade
                trade = self.generate_trade(trade_id)
                
                self.producer.produce(
                    topic=TOPIC_TRADES,
                    key=str(trade["trade_id"]),
                    value=json.dumps(trade),
                    callback=self.delivery_report
                )
                
                self.trades_sent += 1
                self.sent_trade_ids.append(trade_id)
                
                # Poll to trigger callbacks (non-blocking)
                self.producer.poll(0)
                
                # Log progress every 1 second
                current_time = time.time()
                if current_time - last_log_time >= 1.0:
                    elapsed = current_time - self.start_time
                    current_tps = self.trades_sent / elapsed if elapsed > 0 else 0
                    print(f"\rSent: {self.trades_sent:,} | TPS: {current_tps:,.0f} | Elapsed: {elapsed:.1f}s", 
                          end='', flush=True)
                    last_log_time = current_time
                
                # Sleep if target TPS is set
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                trade_id += 1
                
        except KeyboardInterrupt:
            logger.info("\n\nBenchmark interrupted by user")
        
        finally:
            print("\n")  # New line after progress
            logger.info("Flushing remaining messages...")
            self.producer.flush()
            self.end_time = time.time()
            
            # Final statistics
            self.print_summary()
            self.save_results()
    
    def print_summary(self):
        """Print benchmark summary"""
        duration = self.end_time - self.start_time
        avg_tps = self.trades_sent / duration if duration > 0 else 0
        
        logger.info("=" * 60)
        logger.info("BENCHMARK COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Duration:          {duration:.2f} seconds")
        logger.info(f"Trades Sent:       {self.trades_sent:,}")
        logger.info(f"Trades Acked:      {self.trades_acknowledged:,}")
        logger.info(f"Failed Trades:     {self.failed_trades}")
        logger.info(f"Average TPS:       {avg_tps:,.2f}")
        logger.info(f"Peak TPS (calc):   {avg_tps * 1.1:,.2f} (estimated)")
        logger.info("=" * 60)
    
    def save_results(self):
        """Save benchmark results for verification"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = BENCHMARK_RESULTS_DIR / f"benchmark_run_{timestamp}.json"
        
        results = {
            "timestamp": timestamp,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.end_time - self.start_time,
            "total_trades_sent": self.trades_sent,
            "trades_acknowledged": self.trades_acknowledged,
            "failed_trades": self.failed_trades,
            "average_tps": self.trades_sent / (self.end_time - self.start_time),
            "target_tps": self.target_tps,
            "sent_trade_ids": self.sent_trade_ids,
            "config": {
                "duration": self.duration,
                "total_trades": self.total_trades,
                "target_tps": self.target_tps
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to: {filename}")
        return filename


def main():
    parser = argparse.ArgumentParser(description="Benchmark Producer for Post-Trade Engine")
    
    # Mutually exclusive group: either duration or total trades
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--duration', type=int, help='Duration in seconds')
    group.add_argument('--total', type=int, help='Total number of trades to send')
    
    # Optional target TPS
    parser.add_argument('--tps', type=int, help='Target trades per second (if not set, goes max speed)')
    
    args = parser.parse_args()
    
    # Create and run benchmark
    benchmark = BenchmarkProducer(
        target_tps=args.tps,
        duration=args.duration,
        total_trades=args.total
    )
    
    benchmark.run()


if __name__ == "__main__":
    main()