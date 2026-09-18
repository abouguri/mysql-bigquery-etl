import pytest

from scripts.warehouse_checks import render


def test_sql_identifiers_cannot_inject_statements():
    with pytest.raises(ValueError):
        render('SELECT * FROM `{{project}}.{{dataset}}.orders`', 'fixture-project', 'x`; DROP TABLE y')
