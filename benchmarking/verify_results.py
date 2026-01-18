"""
Verify Results - Data integrity verification after benchmark.

This script compares the benchmark producer's sent trades against what's
actually stored in the database to verify system correctness.

Usage:
    python verify_results.py
    python verify_results.py --file benchmark_run_20260118_143022.json
"""

import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import sqlite3
import json
import argparse
from datetime import datetime

from config import setup_logger, DB_PATH, BENCHMARK_RESULTS_DIR

# Setup logger
logger = setup_logger("verify_results", "verify_results.log")


class ResultsVerifier:
    def __init__(self, benchmark_file=None):
        """
        Initialize results verifier.
        
        Args:
            benchmark_file: Path to specific benchmark results file (if None, uses most recent)
        """
        self.benchmark_file = benchmark_file
        self.benchmark_data = None
        self.db_trades = []
        
    def find_latest_benchmark(self):
        """Find the most recent benchmark results file"""
        try:
            files = list(BENCHMARK_RESULTS_DIR.glob("benchmark_run_*.json"))
            if not files:
                logger.error(f"No benchmark files found in {BENCHMARK_RESULTS_DIR}")
                return None
            
            # Sort by modification time, get most recent
            latest = max(files, key=lambda f: f.stat().st_mtime)
            logger.info(f"Using latest benchmark file: {latest.name}")
            return latest
        except Exception as e:
            logger.error(f"Error finding benchmark files: {e}")
            return None
    
    def load_benchmark_data(self):
        """Load benchmark results from JSON file"""
        if self.benchmark_file is None:
            self.benchmark_file = self.find_latest_benchmark()
        
        if self.benchmark_file is None:
            print("\n❌ ERROR: No benchmark file found!")
            print(f"   Please run benchmark_producer.py first")
            return False
        
        try:
            with open(self.benchmark_file, 'r') as f:
                self.benchmark_data = json.load(f)
            
            logger.info(f"Loaded benchmark data from: {self.benchmark_file}")
            return True
        except Exception as e:
            logger.error(f"Error loading benchmark file: {e}")
            print(f"\n❌ ERROR: Could not load benchmark file: {e}")
            return False
    
    def load_db_trades(self):
        """Load all trades from database"""
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            
            # Get all trade IDs and sequence numbers
            cur.execute("SELECT trade_id, symbol, quantity, price, side FROM trades ORDER BY trade_id")
            self.db_trades = cur.fetchall()
            
            conn.close()
            logger.info(f"Loaded {len(self.db_trades)} trades from database")
            return True
        except sqlite3.OperationalError as e:
            logger.error(f"Database error: {e}")
            print(f"\n❌ ERROR: Could not access database: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            print(f"\n❌ ERROR: {e}")
            return False
    
    def verify(self):
        """Perform verification and return results"""
        # Load data
        if not self.load_benchmark_data():
            return None
        
        if not self.load_db_trades():
            return None
        
        # Extract data from benchmark
        sent_trade_ids = set(self.benchmark_data['sent_trade_ids'])
        total_sent = self.benchmark_data['total_trades_sent']
        
        # Extract data from database
        db_trade_ids = set([trade[0] for trade in self.db_trades])
        total_in_db = len(self.db_trades)
        
        # Find missing and extra trades
        missing_trades = sent_trade_ids - db_trade_ids
        extra_trades = db_trade_ids - sent_trade_ids
        
        # Check for duplicates
        db_trade_id_list = [trade[0] for trade in self.db_trades]
        duplicates = set([tid for tid in db_trade_id_list if db_trade_id_list.count(tid) > 1])
        
        # Calculate success rate
        success_rate = (total_in_db / total_sent * 100) if total_sent > 0 else 0
        
        results = {
            'benchmark_file': str(self.benchmark_file.name),
            'total_sent': total_sent,
            'total_in_db': total_in_db,
            'missing_count': len(missing_trades),
            'missing_ids': sorted(list(missing_trades))[:20],  # First 20 missing IDs
            'extra_count': len(extra_trades),
            'extra_ids': sorted(list(extra_trades))[:20],
            'duplicate_count': len(duplicates),
            'duplicate_ids': sorted(list(duplicates))[:20],
            'success_rate': success_rate,
            'duration': self.benchmark_data['duration'],
            'avg_tps': self.benchmark_data['average_tps']
        }
        
        return results
    
    def print_report(self, results):
        """Print verification report"""
        if results is None:
            return
        
        print("\n" + "=" * 70)
        print("🔍 VERIFICATION REPORT")
        print("=" * 70)
        print(f"  Benchmark File:    {results['benchmark_file']}")
        print(f"  Benchmark Duration: {results['duration']:.2f} seconds")
        print(f"  Average TPS:        {results['avg_tps']:,.2f}")
        print("-" * 70)
        print(f"  Trades Sent:        {results['total_sent']:,}")
        print(f"  Trades in DB:       {results['total_in_db']:,}")
        print(f"  Missing Trades:     {results['missing_count']:,}")
        print(f"  Extra Trades:       {results['extra_count']:,}")
        print(f"  Duplicate Trades:   {results['duplicate_count']:,}")
        print(f"  Success Rate:       {results['success_rate']:.3f}%")
        print("=" * 70)
        
        # Determine status
        if results['missing_count'] == 0 and results['duplicate_count'] == 0:
            status = "✅ PASSED"
            message = "All trades verified successfully!"
        elif results['missing_count'] > 0 and results['duplicate_count'] == 0:
            status = "⚠️  WARNING"
            message = f"{results['missing_count']} trades missing (possible lag or in-flight)"
        elif results['duplicate_count'] > 0:
            status = "❌ FAILED"
            message = f"Duplicates detected! Idempotency issue."
        else:
            status = "⚠️  WARNING"
            message = "Data inconsistencies detected"
        
        print(f"  Status: {status}")
        print(f"  {message}")
        print("=" * 70)
        
        # Show details if there are issues
        if results['missing_count'] > 0:
            print(f"\n⚠️  Missing Trade IDs (first 20): {results['missing_ids']}")
        
        if results['extra_count'] > 0:
            print(f"\n⚠️  Extra Trade IDs (first 20): {results['extra_ids']}")
        
        if results['duplicate_count'] > 0:
            print(f"\n❌ Duplicate Trade IDs (first 20): {results['duplicate_ids']}")
        
        print("")
        
        # Log results
        logger.info("=" * 60)
        logger.info("VERIFICATION RESULTS")
        logger.info(f"Total Sent: {results['total_sent']:,}")
        logger.info(f"Total in DB: {results['total_in_db']:,}")
        logger.info(f"Missing: {results['missing_count']:,}")
        logger.info(f"Duplicates: {results['duplicate_count']:,}")
        logger.info(f"Success Rate: {results['success_rate']:.3f}%")
        logger.info(f"Status: {status}")
        logger.info("=" * 60)
    
    def save_report(self, results):
        """Save verification report to file"""
        if results is None:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = BENCHMARK_RESULTS_DIR / f"verification_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"📄 Detailed report saved to: {report_file}\n")
        logger.info(f"Report saved to: {report_file}")


def main():
    parser = argparse.ArgumentParser(description="Verify benchmark results against database")
    parser.add_argument('--file', type=str, help='Specific benchmark file to verify (default: latest)')
    
    args = parser.parse_args()
    
    # Determine file path if provided
    benchmark_file = None
    if args.file:
        if pathlib.Path(args.file).exists():
            benchmark_file = pathlib.Path(args.file)
        else:
            benchmark_file = BENCHMARK_RESULTS_DIR / args.file
    
    # Create verifier and run
    verifier = ResultsVerifier(benchmark_file=benchmark_file)
    results = verifier.verify()
    verifier.print_report(results)
    verifier.save_report(results)


if __name__ == "__main__":
    main()