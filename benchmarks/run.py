"""Measure identical transforms/contracts with bounded vs full-frame input."""
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


def worker(rows, batch_size):
    import pandas as pd
    from etl.contracts import transform, validate
    started = time.perf_counter()
    revenue = Decimal('0')
    accepted = 0
    for start in range(1, rows + 1, batch_size):
        ids = list(range(start, min(start + batch_size, rows + 1)))
        source = pd.DataFrame({
            'order_id': ids, 'user_id': [1 + i % 1000 for i in ids],
            'product_id': [1 + i % 100 for i in ids], 'quantity': [1 + i % 4 for i in ids],
            'unit_price': [Decimal('19.95')] * len(ids),
            'created_at': [datetime(2026, 1, 1, tzinfo=timezone.utc)] * len(ids),
            'updated_at': [datetime(2026, 1, 1, tzinfo=timezone.utc)] * len(ids),
        })
        result = validate(transform(source, ['calculate_totals', 'categorize_orders']), 'orders')
        revenue += sum(result.total_amount, Decimal('0'))
        accepted += len(result)
    elapsed = time.perf_counter() - started
    expected_units = (rows // 4) * 10 + sum(1 + i % 4 for i in range(1, rows % 4 + 1))
    assert accepted == rows and revenue == Decimal('19.95') * expected_units
    return dict(rows=rows, batch_size=batch_size, elapsed_seconds=round(elapsed, 6),
                rows_per_second=round(rows / elapsed, 2), peak_rss_mib=round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
                revenue_usd=str(revenue), python=platform.python_version(), machine=platform.machine(),
                visible_cpu_count=os.cpu_count(), cpu_limit=os.getenv('BENCH_CPU_LIMIT', 'unspecified'),
                memory_limit=os.getenv('BENCH_MEMORY_LIMIT', 'unspecified'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, nargs='+', default=[100000, 1000000])
    parser.add_argument('--batch-size', type=int, default=10000)
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--single', action='store_true')
    parser.add_argument('--output', type=Path, default=Path('benchmarks/results/local.csv'))
    args = parser.parse_args()
    if min(args.rows) <= 0 or args.batch_size <= 0 or args.repetitions <= 0:
        parser.error('Counts must be positive')
    if args.single:
        print(json.dumps(worker(args.rows[0], args.batch_size)))
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    commit = os.getenv('BENCH_COMMIT', 'unknown')
    hashes = {name: hashlib.sha256(Path(path).read_bytes()).hexdigest() for name, path in [
        ('benchmark_sha256', __file__), ('contracts_sha256', 'etl/contracts.py')
    ]}
    with args.output.open('w', newline='') as output:
        writer = None
        for rows in args.rows:
            for mode, size in [('bounded', args.batch_size), ('full_frame', rows)]:
                for repetition in range(1, args.repetitions + 1):
                    result = subprocess.run([sys.executable, '-m', 'benchmarks.run', '--single', '--rows', str(rows), '--batch-size', str(size)], check=True, capture_output=True, text=True)
                    record = dict(timestamp=datetime.now(timezone.utc).isoformat(), base_commit=commit,
                                  mode=mode, repetition=repetition, **json.loads(result.stdout), **hashes)
                    if writer is None:
                        writer = csv.DictWriter(output, fieldnames=list(record))
                        writer.writeheader()
                    writer.writerow(record)
                    output.flush()
                    print(f"{mode}: {rows} rows, sample {repetition}, {record['elapsed_seconds']}s, {record['peak_rss_mib']} MiB", flush=True)


if __name__ == '__main__':
    main()
