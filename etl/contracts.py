"""Version 1 commerce contracts. Source dates are UTC; money is decimal USD."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN

import pandas as pd
from google.cloud import bigquery


SOURCE_FIELDS = {
    "users": [("user_id", "INTEGER"), ("email", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
    "products": [("product_id", "INTEGER"), ("category", "STRING"), ("price", "NUMERIC"), ("updated_at", "TIMESTAMP")],
    "orders": [("order_id", "INTEGER"), ("user_id", "INTEGER"), ("product_id", "INTEGER"), ("quantity", "INTEGER"), ("unit_price", "NUMERIC"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")],
}


def schema(table):
    fields = SOURCE_FIELDS[table] + ([('total_amount', 'NUMERIC'), ('order_size', 'STRING')] if table == 'orders' else [])
    return [bigquery.SchemaField(name, kind, mode="REQUIRED") for name, kind in fields]


def money(value):
    try:
        result = Decimal(str(value))
        if not result.is_finite() or abs(result) >= Decimal('10000000000'):
            raise ValueError("Invalid monetary amount")
        if result != result.quantize(Decimal('0.01'), rounding=ROUND_HALF_EVEN):
            raise ValueError("Money must have at most two decimal places")
        return result.quantize(Decimal('0.01'))
    except (InvalidOperation, TypeError):
        raise ValueError("Invalid monetary amount") from None


def validate(frame, table):
    expected = schema(table)
    names = [field.name for field in expected]
    if set(frame.columns) != set(names):
        raise ValueError("Unexpected or missing columns")
    result = frame[names].copy()
    for field in expected:
        values = result[field.name]
        if values.isna().any():
            raise ValueError(f"Null in required field: {field.name}")
        if field.field_type == 'TIMESTAMP':
            result[field.name] = pd.to_datetime(values, errors='raise', utc=True)
        elif field.field_type == 'NUMERIC':
            result[field.name] = values.map(money)
        elif field.field_type == 'INTEGER':
            if not all(isinstance(x, int) and not isinstance(x, bool) for x in values.tolist()):
                raise ValueError(f"Invalid integer: {field.name}")
            if not values.empty and ((values < -(2**63)).any() or (values >= 2**63).any()):
                raise ValueError("Integer out of range")
            result[field.name] = values.astype('int64')
        elif not all(isinstance(x, str) for x in values):
            raise ValueError(f"Invalid string: {field.name}")
    return result


def transform(frame, transformations):
    known = {'clean_emails', 'standardize_dates', 'calculate_totals', 'categorize_orders', 'standardize_categories', 'format_prices'}
    for transformation in transformations:
        if not callable(transformation) and transformation not in known:
            raise ValueError("Unknown transformation")
    result = frame.copy()
    for action in transformations:
        if callable(action):
            result = action(result)
        elif action == 'clean_emails':
            result['email'] = result['email'].str.lower().str.strip()
        elif action == 'standardize_dates':
            for col in [name for name in result if name.endswith('_at')]:
                result[col] = pd.to_datetime(result[col], errors='raise', utc=True)
        elif action == 'calculate_totals':
            result['unit_price'] = result['unit_price'].map(money)
            result['total_amount'] = (result['quantity'] * result['unit_price']).map(money)
        elif action == 'categorize_orders':
            result['order_size'] = result['total_amount'].map(
                lambda x: 'Refund' if x < 0 else 'Small' if x <= 50 else 'Medium' if x <= 200 else 'Large'
            )
        elif action == 'standardize_categories':
            result['category'] = result['category'].str.strip().str.title()
        elif action == 'format_prices':
            result['price'] = result['price'].map(money)
    return result
