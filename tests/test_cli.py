import pytest
from main import arguments


@pytest.mark.parametrize('argv', [
    ['--replay-from', '2026-01-01T00:00:00Z'],
    ['--replay-from', '2026-01-01'],
    ['--table', 'typo'],
    ['--table', 'orders', '--reconcile', '--replay-from', '2026-01-01T00:00:00Z', '--replay-until', '2026-01-02T00:00:00Z'],
])
def test_invalid_cli_bounds(argv):
    with pytest.raises(SystemExit):
        arguments(argv)


def test_explicit_replay_window():
    args = arguments(['--table', 'orders', '--replay-from', '2026-01-01T00:00:00Z', '--replay-until', '2026-01-02T00:00:00Z'])
    assert args.replay_from < args.replay_until
