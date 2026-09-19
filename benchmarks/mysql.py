"""Measure real fixture MySQL extraction plus transforms, excluding warehouse writes."""
import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import subprocess
import sys
import time

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from etl.contracts import transform, validate
from etl.source import Source


def engine():
    if os.environ.get('MYSQL_DATABASE') != 'commerce_fixture':
        raise ValueError('Benchmark is restricted to the disposable commerce_fixture database')
    return create_engine(URL.create('mysql+mysqlconnector', username=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD'], host=os.environ['MYSQL_HOST'],
        port=int(os.getenv('MYSQL_PORT', '3306')), database='commerce_fixture'))


def name(rows):
    if rows <= 0:
        raise ValueError('Rows must be positive')
    return f'benchmark_orders_{rows}'


def seed(db, rows):
    # CREATE fails if the name already exists: never overwrite an unknown table.
    with db.begin() as connection:
        connection.execute(text(f'''CREATE TABLE {name(rows)} (
            order_id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL, product_id BIGINT NOT NULL,
            quantity INT NOT NULL, unit_price DECIMAL(12,2) NOT NULL,
            created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL)'''))
    with db.begin() as connection:
        insert = text(f"INSERT INTO {name(rows)} VALUES (:id,:user,:product,:quantity,19.95,'2026-01-01','2026-01-01')")
        for start in range(1, rows + 1, 10000):
            connection.execute(insert, [dict(id=i, user=1+i%1000, product=1+i%100, quantity=1+i%4)
                                        for i in range(start, min(start+10000, rows+1))])


def worker(rows, batch_size):
    db = engine()
    total = Decimal('0')
    count = 0
    with db.connect() as connection:
        version = connection.scalar(text('SELECT VERSION()'))
    started = time.perf_counter()
    try:
        with Source(db, batch_size=batch_size).snapshot(
                dict(mysql_table=name(rows), primary_key='order_id'), full=True) as window:
            for page in window.batches:
                result = validate(transform(page, ['calculate_totals', 'categorize_orders']), 'orders')
                total += sum(result.total_amount, Decimal('0'))
                count += len(result)
        elapsed = time.perf_counter() - started
    finally:
        db.dispose()
    units = rows // 4 * 10 + sum(1+i%4 for i in range(1, rows%4+1))
    assert count == rows and total == Decimal('19.95') * units
    return dict(rows=rows, batch_size=batch_size, elapsed_seconds=round(elapsed, 6),
        rows_per_second=round(rows/elapsed, 2), peak_rss_mib=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024, 2),
        revenue_usd=str(total), python=platform.python_version(), mysql=version, machine=platform.machine())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, nargs='+', default=[100000, 1000000])
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--repetitions', type=int, default=3)
    parser.add_argument('--single', action='store_true')
    parser.add_argument('--output', type=Path, default=Path('/results/mysql.csv'))
    args = parser.parse_args()
    if min(args.rows) <= 0 or args.batch_size <= 0 or args.repetitions <= 0 or len(set(args.rows)) != len(args.rows):
        parser.error('Counts must be positive and row sizes unique')
    if args.single:
        print(json.dumps(worker(args.rows[0], args.batch_size)))
        return
    db = engine()
    hashes = {Path(path).name + '_sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest()
              for path in ['benchmarks/mysql.py', 'etl/source.py', 'etl/contracts.py']}
    samples = []
    try:
        for rows in args.rows:
            seed(db, rows)
            try:
                print(f'Seeded {rows} real MySQL rows', flush=True)
                for repetition in range(1, args.repetitions + 1):
                    # Alternate order to reduce a systematic first-mode cache advantage.
                    sizes = [args.batch_size, rows] if repetition % 2 else [rows, args.batch_size]
                    for batch_size in sizes:
                        result = subprocess.run([sys.executable, '-m', 'benchmarks.mysql', '--single',
                            '--rows', str(rows), '--batch-size', str(batch_size)],
                            capture_output=True, text=True, check=True, timeout=300)
                        sample = json.loads(result.stdout)
                        sample.update(repetition=repetition, utc=datetime.now(timezone.utc).isoformat(),
                            base_commit=os.getenv('BENCH_COMMIT', 'unknown'), worker_cpu_limit='2',
                            worker_memory_limit='2GiB', **hashes)
                        samples.append(sample)
                        args.output.parent.mkdir(parents=True, exist_ok=True)
                        with args.output.open('w') as handle:
                            writer = csv.DictWriter(handle, fieldnames=list(samples[0]))
                            writer.writeheader()
                            writer.writerows(samples)
                        print(f"{rows} rows / page {batch_size} / sample {repetition}: {sample['elapsed_seconds']}s, {sample['peak_rss_mib']} MiB", flush=True)
            finally:
                with db.begin() as connection:
                    connection.execute(text(f'DROP TABLE {name(rows)}'))
    finally:
        db.dispose()


if __name__ == '__main__':
    main()
