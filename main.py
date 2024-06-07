"""Calculations supposed to be done here"""

import requests
import pandas as pd


def get_products():
    """Get products from the API"""
    products = requests.get("https://dummyjson.com/products?limit=200")

    try:
        return products.json()
    except requests.exceptions.JSONDecodeError:
        raise requests.exceptions.JSONDecodeError(
            f"{products.text} {products.status_code}"
        )


def get_expected_data():
    """Get the expected data from the parquet file"""
    return pd.read_parquet("data/product_prices_calculated.parquet").fillna(0).to_dict(
        orient="records"
    )


def get_calculated_price(products):
    """Get the calculated price
    Args:
        products (list): List of products from the API
    """
    return [
        {
            "id": product["id"],
            "final_price": (
                product["price"]
                - (product["price"] * product["discountPercentage"] / 100)
                if product["discountPercentage"]
                else product["price"]
            ),
        }
        for product in products
    ]


def get_products_missed_in_expected_data(products, expected_data):
    """Get the products that are not in the expected data
    Args:

        products (list): List of products from the API
        expected_data (list): List of products from the parquet file
    """
    expected_data_ids = [product["id"] for product in expected_data]

    return [product for product in products if product["id"] not in expected_data_ids]


if __name__ == "__main__":
    actual = get_products()
    expected = get_expected_data()
    calculated_prices = get_calculated_price(actual["products"])

    # What product is the most expensive according to actual data?
    most_expensive = max(calculated_prices, key=lambda x: x["final_price"])
    print(f"What product is the most expensive according to actual data? {most_expensive}")
    print("="*40)

    # What product is missing in expected data?
    missed = get_products_missed_in_expected_data(actual["products"], expected)
    print(f"What product is missing in expected data? {missed}")
    print("="*40)

    # For how many rows final price in expected data matches with calculated price from actual data?
    key = "final_price"
    matched = len(
        # A trick here - we should round actual by digits amount of expected to match the expected
        [
            1
            for actual in calculated_prices
            for exp in expected
            if round(actual[key], len(str(exp[key]).split(".")[1])) == exp[key]
        ]
    )
    print(f"For how many rows final price in expected data matches with calculated price from actual data? {matched}")
