from decimal import Decimal

import pandas as pd
import pytest

from etl.contracts import money, transform, validate


@pytest.mark.parametrize('value', ['NaN', 'Infinity', '0.001', '10000000000', 'bad'])
def test_invalid_money_fails(value):
    with pytest.raises(ValueError):
        money(value)


def test_exact_money_and_refund_boundaries():
    frame = pd.DataFrame({'quantity': [3, -1, 0, 1], 'unit_price': ['0.10', '50', '5', '200']})
    output = transform(frame, ['calculate_totals', 'categorize_orders'])
    assert output.total_amount.tolist() == [Decimal('0.30'), Decimal('-50'), Decimal('0'), Decimal('200')]
    assert output.order_size.tolist() == ['Small', 'Refund', 'Small', 'Medium']


def test_unknown_transform_rejected_even_for_empty_input():
    with pytest.raises(ValueError, match='Unknown'):
        transform(pd.DataFrame(), ['typo'])


def test_invalid_date_is_not_coerced_to_null():
    with pytest.raises(ValueError):
        transform(pd.DataFrame({'created_at': ['not-a-date']}), ['standardize_dates'])


def test_schema_drift_is_rejected():
    with pytest.raises(ValueError, match='columns'):
        validate(pd.DataFrame({'user_id': [1]}), 'users')
