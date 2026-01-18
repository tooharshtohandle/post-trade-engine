"""
Metrics Collector - Real-time monitoring of benchmark performance.

This script monitors the SQLite database and displays real-time throughput metrics.
Run this in parallel with the benchmark producer to see live performance.

Usage:
    python metrics_collector.py
"""

import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import sqlite3
import time
from datetime import datetime

from config import setup_logger, DB_PATH

# Setup logger
logger = setup_logger("metrics_collector", "metrics_collector.log")


class MetricsCollector:
    def __init__(self, update_interval=1.0):
        """
        Initialize metrics collector.
        
        Args:
            update_interval: How often to update metrics (in seconds)
        """
        self.update_interval = update_interval
        self.start_time = None
        self.previous_count = 0
        self.trade_counts = []  # History for calculating statistics
        self.tps_samples = []
        
    def get_trade_count(self):
        """Query database for total trade count"""
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM trades")
            count = cur.fetchone()[0]
            conn.close()
            return count
        except sqlite3.OperationalError as e:
            # Database might not exist yet or table not created
            logger.warning(f"Database error: {e}")
            return 0
        except Exception as e:
            logger.error(f"Unexpected error querying database: {e}")
            return 0
    
    def calculate_stats(self):
        """Calculate statistical metrics"""
        if not self.tps_samples:
            return 0, 0, 0
        
        avg_tps = sum(self.tps_samples) / len(self.tps_samples)
        max_tps = max(self.tps_samples)
        min_tps = min(self.tps_samples)
        
        return avg_tps, max_tps, min_tps
    
    def format_dashboard(self, elapsed, total_count, current_tps, avg_tps, max_tps):
        """Format the dashboard display"""
        lines = [
            "",
            "=" * 70,
            "📊 REAL-TIME METRICS DASHBOARD",
            "=" * 70,
            f"  Time Elapsed:     {elapsed:>8.1f} seconds",
            f"  Trades in DB:     {total_count:>12,}",
            f"  Current TPS:      {current_tps:>12,.0f} trades/sec",
            f"  Average TPS:      {avg_tps:>12,.0f} trades/sec",
            f"  Peak TPS:         {max_tps:>12,.0f} trades/sec",
            "=" * 70,
            "  Press Ctrl+C to stop monitoring...",
            ""
        ]
        return "\n".join(lines)
    
    def clear_screen(self):
        """Clear the console (works on Windows and Unix)"""
        import os
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def run(self):
        """Run the metrics collector"""
        logger.info("Metrics Collector started")
        logger.info(f"Monitoring database: {DB_PATH}")
        logger.info(f"Update interval: {self.update_interval}s")
        
        print("\n🚀 Starting metrics collection...")
        print("⏳ Waiting for trades to appear in database...\n")
        
        self.start_time = time.time()
        
        try:
            while True:
                current_time = time.time()
                elapsed = current_time - self.start_time
                
                # Get current trade count
                current_count = self.get_trade_count()
                
                # Calculate current TPS
                trades_since_last = current_count - self.previous_count
                current_tps = trades_since_last / self.update_interval
                
                # Store sample (but only if we have trades)
                if current_count > 0:
                    self.tps_samples.append(current_tps)
                    self.trade_counts.append(current_count)
                
                # Calculate statistics
                avg_tps, max_tps, min_tps = self.calculate_stats()
                
                # Clear screen and display dashboard
                self.clear_screen()
                dashboard = self.format_dashboard(
                    elapsed, current_count, current_tps, avg_tps, max_tps
                )
                print(dashboard)
                
                # Log to file
                logger.info(
                    f"Elapsed: {elapsed:.1f}s | "
                    f"Total: {current_count:,} | "
                    f"Current TPS: {current_tps:,.0f} | "
                    f"Avg TPS: {avg_tps:,.0f}"
                )
                
                # Update for next iteration
                self.previous_count = current_count
                
                # Sleep until next update
                time.sleep(self.update_interval)
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Metrics collection stopped by user")
            self.print_final_summary()
        except Exception as e:
            logger.error(f"Error during metrics collection: {e}")
            raise
    
    def print_final_summary(self):
        """Print final summary when stopping"""
        if not self.trade_counts:
            print("\n❌ No trades were recorded during monitoring")
            return
        
        total_trades = self.trade_counts[-1] if self.trade_counts else 0
        elapsed = time.time() - self.start_time
        avg_tps, max_tps, min_tps = self.calculate_stats()
        
        print("\n" + "=" * 70)
        print("📈 FINAL METRICS SUMMARY")
        print("=" * 70)
        print(f"  Total Monitoring Time:  {elapsed:.2f} seconds")
        print(f"  Total Trades Recorded:  {total_trades:,}")
        print(f"  Average TPS:            {avg_tps:,.2f}")
        print(f"  Peak TPS:               {max_tps:,.2f}")
        print(f"  Minimum TPS:            {min_tps:,.2f}")
        print("=" * 70)
        
        logger.info("=" * 60)
        logger.info("FINAL SUMMARY")
        logger.info(f"Total Monitoring Time: {elapsed:.2f}s")
        logger.info(f"Total Trades: {total_trades:,}")
        logger.info(f"Average TPS: {avg_tps:,.2f}")
        logger.info(f"Peak TPS: {max_tps:,.2f}")
        logger.info("=" * 60)


def main():
    collector = MetricsCollector(update_interval=1.0)
    collector.run()


if __name__ == "__main__":
    main()