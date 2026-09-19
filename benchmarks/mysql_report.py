"""Summarize recorded MySQL samples without inventing percentile estimates."""
import csv
from pathlib import Path
from statistics import median


def main():
    source = Path('benchmarks/results/mysql.csv')
    with source.open() as handle:
        samples = list(csv.DictReader(handle))
    lines = ['# Real MySQL extraction and processing', '',
             'Generated from [raw samples](mysql.csv). Each sample uses a fresh Python process. '
             'Elapsed time includes a consistent source snapshot, SQL queries, MySQL transfer, '
             'DataFrame construction, transformations, validation and exact revenue reconciliation. '
             'It excludes fixture seeding, dependency imports and destination writes.', '',
             '| Rows | Page size | Samples | Median seconds (min–max) | Median peak worker RSS MiB (min–max) |',
             '|---|---|---|---|---|']
    for rows in sorted({int(sample['rows']) for sample in samples}):
        for batch in sorted({int(sample['batch_size']) for sample in samples if int(sample['rows']) == rows}):
            group = [sample for sample in samples if int(sample['rows']) == rows and int(sample['batch_size']) == batch]
            durations = [float(sample['elapsed_seconds']) for sample in group]
            memory = [float(sample['peak_rss_mib']) for sample in group]
            lines.append(f'| {rows:,} | {batch:,} | {len(group)} | {median(durations):.3f} ({min(durations):.3f}–{max(durations):.3f}) | {median(memory):.2f} ({min(memory):.2f}–{max(memory):.2f}) |')
    largest = max(int(sample['rows']) for sample in samples)
    bounded = [sample for sample in samples if int(sample['rows']) == largest and int(sample['batch_size']) < largest]
    full = [sample for sample in samples if int(sample['rows']) == largest and int(sample['batch_size']) == largest]
    reduction = 100 * (1 - median(float(s['peak_rss_mib']) for s in bounded) / median(float(s['peak_rss_mib']) for s in full))
    lines.extend(['', f'At {largest:,} rows, bounded extraction used **{reduction:.1f}% less median peak worker RSS**.', '',
        'Worker limit: 2 CPUs / 2 GiB. MySQL runs separately in the same Docker network, uses tmpfs, '
        'and has no explicit CPU/memory cap. Worker RSS excludes MySQL and does not represent total system memory. '
        'Data is synthetic and recently seeded; database/OS caches are not reset. Mode order alternates across repetitions. '
        'These samples are not a p95 estimate or a production capacity claim. No BigQuery calls or warehouse publication were timed.', '',
        'Reproduce with `make benchmark-mysql`, then `python3 -m benchmarks.mysql_report`. '
        'The CSV records the base commit, source hashes, versions, measurement timestamps, row counts and reconciled revenue.'])
    Path('benchmarks/results/mysql-summary.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
