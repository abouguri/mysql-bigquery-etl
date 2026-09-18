"""Summarize recorded samples; --plot additionally requires plot-requirements.txt."""
import argparse
import csv
from collections import defaultdict
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=Path('benchmarks/results/local.csv'))
    parser.add_argument('--plot', action='store_true')
    args = parser.parse_args()
    groups = defaultdict(list)
    with args.input.open() as file:
        for row in csv.DictReader(file):
            groups[(int(row['rows']), row['mode'])].append(row)
    sample_counts = ', '.join(str(n) for n in sorted({len(v) for v in groups.values()}))
    date = min(row['timestamp'] for values in groups.values() for row in values)[:10]
    lines = ['# Local benchmark results', '', f'Recorded {date}. {sample_counts} fresh-process samples per configuration; resource limits are recorded per sample in the CSV.', '',
             '| Rows | Mode | Median processing seconds | Range (s) | Median peak RSS (MiB) | RSS range (MiB) |',
             '|---:|---|---:|---:|---:|---:|']
    for (count, mode), rows in sorted(groups.items()):
        seconds = [float(row['elapsed_seconds']) for row in rows]
        rss = [float(row['peak_rss_mib']) for row in rows]
        lines.append(f'| {count:,} | {mode} | {statistics.median(seconds):.3f} | {min(seconds):.3f}–{max(seconds):.3f} | {statistics.median(rss):.2f} | {min(rss):.2f}–{max(rss):.2f} |')
    size = max(count for count, _ in groups)
    bounded = groups[(size, 'bounded')]
    full = groups[(size, 'full_frame')]
    memory_reduction = 1 - statistics.median(float(r['peak_rss_mib']) for r in bounded) / statistics.median(float(r['peak_rss_mib']) for r in full)
    overhead = statistics.median(float(r['elapsed_seconds']) for r in bounded) / statistics.median(float(r['elapsed_seconds']) for r in full) - 1
    lines += ['', f'At {size:,} rows, bounded processing used **{memory_reduction:.1%} less median peak process RSS**, with **{overhead:.1%} greater median processing time**, than a full DataFrame using the same transformations/contracts.', '',
              'These are local synthetic processing measurements, not end-to-end ETL throughput, original-code speedup, production capacity, BigQuery cost or a freshness SLO. Timing includes fixture generation and validation; peak RSS includes imports. Shared-host contention and sequential experiment ordering limit comparisons. See [methodology](../README.md) and [raw samples](local.csv).', '',
              'The base commit predates this benchmark addition; recorded source hashes identify the exact benchmark and contract code. Every sample passed row-count and exact-revenue assertions. No p95 claim is derived from five samples.']
    (args.input.parent / 'summary.md').write_text('\n'.join(lines)+'\n')
    if not args.plot:
        return
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size': 10, 'svg.fonttype': 'none'})
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
    counts = sorted({count for count, _ in groups})
    for axis, metric, title in [(axes[0], 'peak_rss_mib', 'Peak process memory (MiB)'), (axes[1], 'elapsed_seconds', 'Processing duration (seconds)')]:
        for index, (mode, label, color) in enumerate([('bounded', '10k-row pages', '#16856f'), ('full_frame', 'Full DataFrame', '#4667ac')]):
            values = [[float(r[metric]) for r in groups[(count, mode)]] for count in counts]
            medians = [statistics.median(v) for v in values]
            error = [[median - min(v) for median, v in zip(medians, values)], [max(v) - median for median, v in zip(medians, values)]]
            axis.bar([n + (index - .5) * .35 for n in range(len(counts))], medians, width=.35, label=label, color=color, yerr=error, capsize=4)
        axis.set_xticks(range(len(counts)), [f'{n:,} rows' for n in counts])
        axis.set_title(title)
        axis.spines[['top', 'right']].set_visible(False)
        axis.set_ylim(bottom=0)
        axis.legend(frameon=False)
    fig.suptitle('Memory and runtime: bounded pages versus a full DataFrame', fontsize=14)
    fig.text(.5, .02, f'Local synthetic workload · medians and min–max across {sample_counts} samples · excludes MySQL/BigQuery I/O', ha='center', fontsize=9)
    fig.tight_layout(rect=(0, .05, 1, .92))
    fig.savefig(args.input.parent / 'comparison.svg', metadata={'Date': None})
    fig.savefig(args.input.parent / 'comparison.png', dpi=160)
    plt.close(fig)


if __name__ == '__main__':
    main()
