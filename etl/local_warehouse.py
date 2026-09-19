"""SQLite reference backend: durable local behavior, not a BigQuery emulator.

Money is stored as integer cents. Publication, ownership and progress share one
SQLite transaction. SQLite serializes writers for the entire database file.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import uuid

from config.config import identifier
from etl.warehouse import Claim


class LeaseBusy(RuntimeError):
    pass


class StaleClaim(RuntimeError):
    pass


def stamp(value):
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds') if value is not None else None


class LocalWarehouse:
    def __init__(self, path, lease_seconds=30, hook=None):
        if lease_seconds <= 0:
            raise ValueError('Lease duration must be positive')
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lease_seconds = lease_seconds
        self.hook = hook or (lambda phase: None)

    @contextmanager
    def connection(self, write=False):
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA busy_timeout=10000')
        connection.execute('PRAGMA synchronous=FULL')
        try:
            if write:
                connection.execute('BEGIN IMMEDIATE')
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def now(connection):
        return connection.execute("SELECT (julianday('now') - 2440587.5) * 86400.0").fetchone()[0]

    def bootstrap(self, tables):
        names = [identifier(table['mysql_table']) for table in tables]
        if not names or len(names) != len(set(names)):
            raise ValueError('Table names must be nonempty and unique')
        with self.connection(write=True) as connection:
            connection.execute('''CREATE TABLE IF NOT EXISTS _etl_state (
                table_name TEXT PRIMARY KEY, generation INTEGER NOT NULL DEFAULT 0,
                owner TEXT, lease_until REAL NOT NULL DEFAULT 0,
                watermark_id INTEGER NOT NULL DEFAULT 0, watermark_at TEXT)''')
            connection.execute('''CREATE TABLE IF NOT EXISTS _etl_runs (
                run_id TEXT PRIMARY KEY, table_name TEXT NOT NULL, status TEXT NOT NULL,
                row_count INTEGER, upper_id INTEGER, upper_time TEXT)''')
            connection.execute('''CREATE TABLE IF NOT EXISTS _etl_contracts (
                table_name TEXT PRIMARY KEY, signature TEXT NOT NULL)''')
            connection.executemany('INSERT OR IGNORE INTO _etl_state(table_name) VALUES (?)', [(name,) for name in names])

    def acquire(self, table):
        table = identifier(table)
        with self.connection(write=True) as connection:
            state = connection.execute('SELECT * FROM _etl_state WHERE table_name=?', (table,)).fetchone()
            if state is None:
                raise ValueError('Unknown state table')
            now = self.now(connection)
            if state['lease_until'] > now:
                raise LeaseBusy('Table already leased')
            if state['owner']:
                connection.execute("UPDATE _etl_runs SET status='EXPIRED' WHERE run_id=? AND status='RUNNING'", (state['owner'],))
                # An expired worker can no longer write or publish this stage.
                connection.execute(f'DROP TABLE IF EXISTS "_stage_{state["owner"]}"')
            run_id = uuid.uuid4().hex
            generation = state['generation'] + 1
            connection.execute('UPDATE _etl_state SET generation=?, owner=?, lease_until=? WHERE table_name=?',
                               (generation, run_id, now + self.lease_seconds, table))
            connection.execute("INSERT INTO _etl_runs(run_id,table_name,status) VALUES (?,?,'RUNNING')", (run_id, table))
            return Claim(table, run_id, generation, state['watermark_id'],
                         datetime.fromisoformat(state['watermark_at']) if state['watermark_at'] else None)

    def guard(self, connection, claim):
        state = connection.execute('SELECT * FROM _etl_state WHERE table_name=?', (claim.table,)).fetchone()
        if state is None or state['owner'] != claim.run_id or state['generation'] != claim.generation or state['lease_until'] <= self.now(connection) or state['watermark_id'] != claim.lower_id:
            raise StaleClaim('Owner expired or superseded')
        return state

    def renew(self, claim):
        with self.connection(write=True) as connection:
            self.guard(connection, claim)
            connection.execute('UPDATE _etl_state SET lease_until=? WHERE table_name=?',
                               (self.now(connection) + self.lease_seconds, claim.table))

    def abort(self, claim):
        """Local errors have a resolvable commit outcome; never undo a success."""
        with self.connection(write=True) as connection:
            connection.execute("UPDATE _etl_runs SET status='FAILED' WHERE run_id=? AND status='RUNNING'", (claim.run_id,))
            connection.execute('UPDATE _etl_state SET owner=NULL, lease_until=0 WHERE table_name=? AND owner=? AND generation=?',
                               (claim.table, claim.run_id, claim.generation))
            connection.execute(f'DROP TABLE IF EXISTS "_stage_{claim.run_id}"')

    @staticmethod
    def encoded(value, field):
        if field.field_type == 'NUMERIC':
            return int(Decimal(str(value)) * 100)
        if field.field_type == 'TIMESTAMP':
            return stamp(value)
        if field.field_type == 'INTEGER':
            return int(value)
        return str(value)

    def publish_batches(self, batches, table, claim, upper_id, upper_time=None):
        from etl.contracts import validate
        if table['mysql_table'] != claim.table or upper_id < claim.lower_id:
            raise ValueError('Invalid claim or checkpoint')
        if upper_time is not None and claim.lower_time is not None and upper_time < claim.lower_time:
            raise ValueError('Timestamp checkpoint regression')
        if not all(c in '0123456789abcdef' for c in claim.run_id) or len(claim.run_id) != 32:
            raise ValueError('Invalid run identity')
        target, key = identifier(table['bigquery_table']), identifier(table['primary_key'])
        fields = table['schema']
        names = [identifier(field.name) for field in fields]
        signature = json.dumps([(field.name, field.field_type, field.mode) for field in fields])
        columns = ', '.join(f'"{name}"' for name in names)
        definitions = ', '.join(f'"{field.name}" {"INTEGER" if field.field_type in {"INTEGER", "NUMERIC"} else "TEXT"} NOT NULL' + (' PRIMARY KEY' if field.name == key else '') for field in fields)
        stage = f'_stage_{claim.run_id}'
        try:
            with self.connection(write=True) as connection:
                finished = connection.execute('SELECT * FROM _etl_runs WHERE run_id=?', (claim.run_id,)).fetchone()
                if finished is not None and finished['status'] == 'SUCCEEDED':
                    if finished['table_name'] != claim.table or finished['upper_id'] != upper_id or finished['upper_time'] != stamp(upper_time):
                        raise ValueError('Completed run identity cannot be reused for new bounds')
                    return finished['row_count']
                self.guard(connection, claim)
                previous = connection.execute('SELECT signature FROM _etl_contracts WHERE table_name=?', (target,)).fetchone()
                if previous and previous[0] != signature:
                    raise ValueError('Destination schema migration required')
                if not previous:
                    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (target,)).fetchone():
                        raise ValueError('Unmanaged destination table')
                    connection.execute(f'CREATE TABLE "{target}" ({definitions})')
                    connection.execute('INSERT INTO _etl_contracts VALUES (?,?)', (target, signature))
                connection.execute(f'DROP TABLE IF EXISTS "{stage}"')
                connection.execute(f'CREATE TABLE "{stage}" ({definitions})')
            count = 0
            for frame in batches:
                frame = validate(frame, table['mysql_table'])
                values = [tuple(self.encoded(value, field) for value, field in zip(row, fields)) for row in frame[names].itertuples(index=False, name=None)]
                with self.connection(write=True) as connection:
                    self.guard(connection, claim)
                    connection.executemany(f'INSERT INTO "{stage}" ({columns}) VALUES ({", ".join("?" for _ in fields)})', values)
                    connection.execute('UPDATE _etl_state SET lease_until=? WHERE table_name=?', (self.now(connection) + self.lease_seconds, claim.table))
                count += len(values)
            self.hook('after_stage')
            with self.connection(write=True) as connection:
                self.guard(connection, claim)
                if connection.execute(f'SELECT COUNT(*) FROM "{stage}"').fetchone()[0] != count:
                    raise ValueError('Staged row count mismatch')
                if table['incremental']:
                    updates = ', '.join(f'"{name}"=excluded."{name}"' for name in names if name != key)
                    connection.execute(f'''INSERT INTO "{target}" ({columns}) SELECT {columns} FROM "{stage}" WHERE TRUE
                        ON CONFLICT("{key}") DO UPDATE SET {updates} WHERE excluded.updated_at >= "{target}".updated_at''')
                else:
                    connection.execute(f'DELETE FROM "{target}"')
                    connection.execute(f'INSERT INTO "{target}" SELECT * FROM "{stage}"')
                connection.execute('UPDATE _etl_state SET watermark_id=?, watermark_at=COALESCE(?,watermark_at), owner=NULL, lease_until=0 WHERE table_name=?', (upper_id, stamp(upper_time), claim.table))
                connection.execute("UPDATE _etl_runs SET status='SUCCEEDED',row_count=?,upper_id=?,upper_time=? WHERE run_id=?", (count, upper_id, stamp(upper_time), claim.run_id))
                connection.execute(f'DROP TABLE "{stage}"')
                self.hook('before_commit')
            self.hook('after_commit')
            return count
        except Exception:
            self.abort(claim)
            raise

    def report(self):
        with self.connection() as connection:
            counts = {name: connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
                      for (name,) in connection.execute('SELECT table_name FROM _etl_contracts ORDER BY table_name')}
            # Python integer summation avoids SQLite SUM overflow and floating-point money.
            cents = sum(row[0] for row in connection.execute('SELECT total_amount FROM orders')) if 'orders' in counts else 0
            states = [dict(row) for row in connection.execute('SELECT * FROM _etl_state ORDER BY table_name')]
            runs = [dict(row) for row in connection.execute('SELECT * FROM _etl_runs ORDER BY rowid')]
        return dict(backend='sqlite-local', counts=counts, net_revenue_usd=str(Decimal(cents) / 100), states=states, runs=runs)
