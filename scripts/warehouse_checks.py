"""Render analytics SQL offline; execute only with --execute (billable)."""
import argparse
from pathlib import Path
import re


def render(text, project, dataset):
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,28}[a-z0-9]', project):
        raise ValueError('Invalid project identifier')
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', dataset):
        raise ValueError('Invalid dataset identifier')
    return text.replace('{{project}}', project).replace('{{dataset}}', dataset)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', required=True)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--location', default='US')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--maximum-bytes-billed', type=int, default=1000000000)
    args = parser.parse_args()
    statements = [render((Path(__file__).resolve().parents[1] / 'sql' / name).read_text(), args.project, args.dataset)
                  for name in ['revenue_daily.sql', 'quality_checks.sql']]
    if not args.execute:
        print('\n'.join(statements))
        return
    from google.cloud import bigquery
    with bigquery.Client(project=args.project) as client:
        for sql in statements:
            job = client.query(sql, location=args.location, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=args.maximum_bytes_billed))
            job.result()
            print(f'job_id={job.job_id} bytes_processed={job.total_bytes_processed} bytes_billed={job.total_bytes_billed}')


if __name__ == '__main__':
    main()
