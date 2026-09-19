import pytest
from scripts.local_recovery import run_case, run_overlap


@pytest.mark.parametrize('phase', ['after_stage', 'before_commit', 'after_commit'])
def test_abrupt_worker_exit_recovers_atomically(tmp_path, phase):
    run_case(tmp_path, phase)


def test_separate_workers_cannot_both_acquire(tmp_path):
    run_overlap(tmp_path)
