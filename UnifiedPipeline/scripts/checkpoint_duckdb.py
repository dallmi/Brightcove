"""
Manually checkpoint DuckDB to merge WAL file into main database.

This is useful when a long-running process has been writing to the WAL file
but hasn't checkpointed yet (e.g., if a process is still running or crashed).

Usage:
    # Checkpoint main analytics DB
    python checkpoint_duckdb.py

    # Checkpoint account-specific DB
    python checkpoint_duckdb.py --account Internet

    # Just show stats without checkpointing
    python checkpoint_duckdb.py --stats
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent))

from shared import get_output_paths, setup_logging, init_analytics_db, get_db_stats


def checkpoint_database(db_path: Path, logger):
    """Checkpoint a DuckDB database."""
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return False

    # Check WAL file
    wal_path = Path(str(db_path) + ".wal")
    wal_exists = wal_path.exists()
    wal_size_mb = wal_path.stat().st_size / (1024 * 1024) if wal_exists else 0
    db_size_before = db_path.stat().st_size / (1024 * 1024)

    logger.info(f"Database: {db_path}")
    logger.info(f"  DB size before: {db_size_before:.1f} MB")

    if wal_exists:
        logger.info(f"  WAL file size: {wal_size_mb:.1f} MB")
        logger.info(f"  Status: Changes in WAL not yet merged")
    else:
        logger.info(f"  Status: No WAL file (already checkpointed)")

    # Connect and get stats
    conn = init_analytics_db(db_path)
    stats_before = get_db_stats(conn)

    logger.info(f"\nDatabase contents:")
    logger.info(f"  Total rows: {stats_before['total_rows']:,}")
    logger.info(f"  Unique videos: {stats_before['unique_videos']:,}")
    if stats_before['date_range'][0]:
        logger.info(f"  Date range: {stats_before['date_range'][0]} to {stats_before['date_range'][1]}")

    # Perform checkpoint
    logger.info(f"\n{'='*60}")
    logger.info("Performing CHECKPOINT...")
    logger.info(f"{'='*60}")

    start = datetime.now()
    conn.execute("CHECKPOINT")
    duration = (datetime.now() - start).total_seconds()

    logger.info(f"✓ Checkpoint completed in {duration:.2f} seconds")

    # Close connection to finalize
    conn.close()

    # Check results
    db_size_after = db_path.stat().st_size / (1024 * 1024)
    wal_exists_after = Path(str(db_path) + ".wal").exists()

    logger.info(f"\nResults:")
    logger.info(f"  DB size after: {db_size_after:.1f} MB (Δ {db_size_after - db_size_before:+.1f} MB)")

    if wal_exists_after:
        wal_size_after = Path(str(db_path) + ".wal").stat().st_size / (1024 * 1024)
        logger.info(f"  WAL file after: {wal_size_after:.1f} MB")
        if wal_size_after > 0.1:
            logger.warning("  ⚠️  WAL file still exists - there may be ongoing writes")
    else:
        logger.info(f"  WAL file: Removed (fully merged)")

    return True


def show_stats_only(db_path: Path, logger):
    """Show database stats without checkpointing."""
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return

    wal_path = Path(str(db_path) + ".wal")
    wal_exists = wal_path.exists()
    wal_size_mb = wal_path.stat().st_size / (1024 * 1024) if wal_exists else 0
    db_size = db_path.stat().st_size / (1024 * 1024)

    logger.info(f"Database: {db_path}")
    logger.info(f"  DB size: {db_size:.1f} MB")

    if wal_exists:
        logger.info(f"  WAL file: {wal_size_mb:.1f} MB (needs checkpoint)")
    else:
        logger.info(f"  WAL file: None (fully checkpointed)")

    conn = init_analytics_db(db_path)
    stats = get_db_stats(conn)
    conn.close()

    logger.info(f"\nContents:")
    logger.info(f"  Total rows: {stats['total_rows']:,}")
    logger.info(f"  Unique videos: {stats['unique_videos']:,}")
    if stats['date_range'][0]:
        logger.info(f"  Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")

    if stats['rows_by_account']:
        logger.info(f"\n  Rows by account:")
        for account_id, count in sorted(stats['rows_by_account'].items(), key=lambda x: x[1], reverse=True):
            logger.info(f"    {account_id}: {count:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Checkpoint DuckDB database (merge WAL into main DB)"
    )
    parser.add_argument(
        '--account',
        type=str,
        help='Checkpoint account-specific DB'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show stats only (no checkpoint)'
    )
    args = parser.parse_args()

    paths = get_output_paths()
    logger = setup_logging(paths['logs'], "checkpoint_duckdb")

    # Determine DB path
    if args.account:
        db_path = paths['output'] / f"analytics_{args.account}.duckdb"
    else:
        db_path = paths['output'] / "analytics.duckdb"

    if args.stats:
        show_stats_only(db_path, logger)
    else:
        checkpoint_database(db_path, logger)


if __name__ == "__main__":
    main()
