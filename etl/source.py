"""Bounded keyset pages from a consistent, per-table MySQL snapshot."""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta, timezone

import pandas as pd
from sqlalchemy import text

from config.config import identifier


@dataclass
class Window:
    upper_id: int
    upper_time: object
    batches: object


class Source:
    def __init__(self, engine, batch_size=10000, lookback_seconds=86400):
        if batch_size <= 0 or lookback_seconds < 0:
            raise ValueError('Invalid source batching settings')
        self.engine = engine
        self.batch_size = batch_size
        self.lookback = timedelta(seconds=lookback_seconds)

    @contextmanager
    def snapshot(self, table, watermark=None, full=False, replay=None):
        name, key = identifier(table['mysql_table']), identifier(table['primary_key'])
        with self.engine.connect().execution_options(isolation_level='REPEATABLE READ') as connection:
            with connection.begin():
                connection.exec_driver_sql("SET time_zone = '+00:00'")
                connection.exec_driver_sql('START TRANSACTION WITH CONSISTENT SNAPSHOT')
                now = connection.scalar(text('SELECT UTC_TIMESTAMP(6)')).replace(tzinfo=timezone.utc)
                bounds = connection.execute(text(f'SELECT MIN(`{key}`) first_id, COALESCE(MAX(`{key}`), 0) upper_id FROM `{name}`')).one()
                if bounds.first_id is not None and bounds.first_id <= 0:
                    raise ValueError('Source primary keys must be positive')
                upper_id = int(bounds.upper_id)
                lower = None if watermark is None else watermark - self.lookback
                upper_time = now
                if replay:
                    lower, upper_time = replay
                    if lower > upper_time or upper_time > now:
                        raise ValueError('Replay window must be ordered and not in the future')
                elif watermark is not None and watermark > now:
                    raise ValueError('Source clock is behind the saved checkpoint')

                def pages():
                    after = 0
                    while True:
                        clauses = [f'`{key}` > :after', f'`{key}` <= :upper_id']
                        params = dict(after=after, upper_id=upper_id, batch_size=self.batch_size)
                        if not full or replay:
                            clauses.append('(`updated_at` <= :upper_time OR `updated_at` IS NULL)')
                            params['upper_time'] = upper_time.replace(tzinfo=None)
                            if lower is not None:
                                clauses.append('(`updated_at` >= :lower_time OR `updated_at` IS NULL)')
                                params['lower_time'] = lower.replace(tzinfo=None)
                        sql = text(f'SELECT * FROM `{name}` WHERE {" AND ".join(clauses)} ORDER BY `{key}` LIMIT :batch_size')
                        frame = pd.read_sql(sql, connection, params=params, coerce_float=False)
                        # Yield an empty frame too: schema drift must be detected on empty tables.
                        if frame.empty:
                            if after == 0:
                                yield frame
                            break
                        if frame[key].isna().any() or (frame[key] <= after).any() or frame[key].duplicated().any():
                            raise ValueError('Source keyset invariant failed')
                        after = int(frame[key].iloc[-1])
                        yield frame
                        if len(frame) < self.batch_size:
                            break
                yield Window(upper_id, upper_time, pages())
