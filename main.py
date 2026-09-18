"""CLI for normal ingestion, full reconciliation and bounded current-row replay."""
import argparse
from datetime import datetime, timezone
import logging

from dotenv import load_dotenv

from etl_pipeline import ETLPipeline


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError('Timezone is required')
        return parsed.astimezone(timezone.utc)
    except ValueError as error:
        raise argparse.ArgumentTypeError('Use an ISO timestamp with timezone') from error


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table', choices=['users', 'orders', 'products'])
    parser.add_argument('--reconcile', action='store_true', help='Replace selected tables from consistent source snapshots, repairing deletes')
    parser.add_argument('--replay-from', type=timestamp)
    parser.add_argument('--replay-until', type=timestamp)
    args = parser.parse_args(argv)
    if args.replay_from or args.replay_until:
        if not (args.replay_from and args.replay_until and args.table) or args.reconcile:
            parser.error('Replay requires --table and both time bounds, and cannot use --reconcile')
        if args.replay_from > args.replay_until:
            parser.error('Replay bounds must be ordered')
    return args


def main(argv=None):
    args = arguments(argv)
    load_dotenv()
    try:
        replay = (args.replay_from, args.replay_until) if args.replay_from else None
        return 0 if ETLPipeline().run_pipeline(args.table, args.reconcile, replay) else 1
    except Exception as error:
        logging.error('Application error: %s', type(error).__name__)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
