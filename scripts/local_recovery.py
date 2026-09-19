"""Reproducible process-crash and overlapping-worker checks for SQLite only."""
import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from etl.contracts import schema, validate
from etl.local_warehouse import LocalWarehouse, LeaseBusy, StaleClaim
from etl.warehouse import Claim


def table():
    return dict(mysql_table='products', bigquery_table='products', primary_key='product_id',
                incremental=False, schema=schema('products'))


def frame():
    import pandas as pd
    return validate(pd.DataFrame(dict(product_id=[1], category=['Books'], price=['19.95'],
                                     updated_at=['2026-01-01'])), 'products')


def child(path, phase, barrier):
    if phase == 'overlap':
        deadline = time.monotonic() + 20
        while not Path(barrier).exists():
            if time.monotonic() > deadline:
                raise TimeoutError('Start barrier not released')
            time.sleep(.01)
        warehouse = LocalWarehouse(path)
        try:
            warehouse.acquire('products')
            print('acquired', flush=True)
        except LeaseBusy:
            print('busy', flush=True)
        return
    def crash(current):
        if current == phase:
            os._exit(86)  # Deliberately skip finally blocks and connection cleanup.
    warehouse = LocalWarehouse(path, lease_seconds=2, hook=crash)
    warehouse.publish_batches([frame()], table(), warehouse.acquire('products'), 1)
    raise AssertionError('Crash hook was not reached')


def command(path, phase, barrier='unused'):
    return [sys.executable, '-m', 'scripts.local_recovery', '--worker', str(path), phase, str(barrier)]


def run_case(directory, phase):
    path = Path(directory) / (phase + '.sqlite')
    warehouse = LocalWarehouse(path, lease_seconds=2)
    warehouse.bootstrap([table()])
    result = subprocess.run(command(path, phase), capture_output=True, text=True, timeout=30)
    assert result.returncode == 86, result.stderr
    started = time.monotonic()
    report = warehouse.report()
    committed = phase == 'after_commit'
    assert report['counts']['products'] == int(committed)
    assert report['states'][0]['watermark_id'] == int(committed)
    assert report['runs'][0]['status'] == ('SUCCEEDED' if committed else 'RUNNING')
    original = Claim('products', report['runs'][0]['run_id'], 1, 0)
    if committed:
        def forbidden():
            raise AssertionError('Committed identity must resolve without rereading source')
            yield
        assert warehouse.publish_batches(forbidden(), table(), original, 1) == 1
    else:
        deadline = started + 10
        while True:
            try:
                current = warehouse.acquire('products')
                break
            except LeaseBusy:
                if time.monotonic() >= deadline:
                    raise TimeoutError('Lease did not expire')
                time.sleep(.05)
        try:
            warehouse.renew(original)
        except StaleClaim:
            pass
        else:
            raise AssertionError('Expired owner renewed its lease')
        warehouse.publish_batches([frame()], table(), current, 1)
    final = warehouse.report()
    assert final['counts']['products'] == 1
    assert final['states'][0]['watermark_id'] == 1
    with warehouse.connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM sqlite_master WHERE name LIKE '_stage_%'").fetchone()[0] == 0
    return dict(scenario=phase, recovery_seconds=round(time.monotonic() - started, 4),
                test_lease_seconds=2, rows=1, checkpoint=1, result='passed')


def run_overlap(directory):
    path = Path(directory) / 'overlap.sqlite'
    barrier = Path(directory) / 'start'
    warehouse = LocalWarehouse(path)
    warehouse.bootstrap([table()])
    workers = [subprocess.Popen(command(path, 'overlap', barrier), stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True) for _ in range(2)]
    try:
        barrier.touch()
        results = []
        for worker in workers:
            out, error = worker.communicate(timeout=30)
            assert worker.returncode == 0, error
            results.append(out.strip())
        assert sorted(results) == ['acquired', 'busy'], results
        assert len(warehouse.report()['runs']) == 1
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
                worker.wait()
    return dict(scenario='overlap', recovery_seconds='', test_lease_seconds=30,
                rows=0, checkpoint=0, result='one owner; one rejected')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker', nargs=3, metavar=('PATH', 'PHASE', 'BARRIER'))
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.worker:
        child(*args.worker)
        return
    with tempfile.TemporaryDirectory() as directory:
        results = [run_case(directory, phase) for phase in ('after_stage', 'before_commit', 'after_commit')]
        results.append(run_overlap(directory))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open('w') as handle:
            writer = csv.DictWriter(handle, fieldnames=list(results[0]))
            writer.writeheader()
            writer.writerows(results)
    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
