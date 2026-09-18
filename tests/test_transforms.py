import pandas as pd


def test_commerce_transforms(pipeline):
    users = pipeline.transform_data(
        pd.DataFrame({"email": [" ALICE@example.test "]}), ["clean_emails"]
    )
    assert users.email.tolist() == ["alice@example.test"]
    orders = pipeline.transform_data(
        pd.DataFrame({"quantity": [2, 1], "unit_price": [19.95, 250]}),
        ["calculate_totals", "categorize_orders"],
    )
    assert orders.total_amount.tolist() == [39.9, 250]
    assert orders.order_size.tolist() == ["Small", "Large"]


def test_empty_transform(pipeline):
    assert pipeline.transform_data(pd.DataFrame(columns=["email"]), ["clean_emails"]).empty
