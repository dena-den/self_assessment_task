import requests
import json
import pandas as pd

class ProductDataProcessor:
    def __init__(self, api_url, local_path):
        '''
        Initialize the class with the API URL and the local path of the parquet file
        Args:
            api_url (str): The URL of the API to fetch the data from
            local_path (str): The local path of the parquet file containing the expected data
        '''
        self.api_url = api_url
        self.local_path = local_path

    def fetch_data(self):
        '''Fetch the data from the API and store it in the data attribute'''
        response = requests.get(f"{self.api_url}?limit=194")
        response.raise_for_status()  # Raise an error for bad status codes
        self.data = response.json()['products']
    
    def process_data(self):
        '''Process the data by calculating the final price'''
        for product in self.data:
            product['final_price'] = product['price'] * (1 - product['discountPercentage'] / 100)
        self.actual_data = self.data

    def load_from_parquet(self):
        '''Load the expected data from the parquet file'''
        self.expected_data = pd.read_parquet(self.local_path).fillna(0).to_dict(
            orient="records"
        )
    
    def get_most_expensive_product(self):
        '''Get the most expensive product from the actual data'''
        most_expensive_product = max(self.actual_data, key=lambda x: x['final_price'])
        print(f"Most Expensive Product: {most_expensive_product['title']}, Price: {most_expensive_product['final_price']}")

    def find_missing_products(self):
        '''Find the missing products in the expected data'''
        actual_ids = {product['id'] for product in self.actual_data}
        expected_ids = {product['id'] for product in self.expected_data}
        missing_ids = actual_ids - expected_ids
        missing_products = [product for product in self.actual_data if product['id'] in missing_ids]
        
        if missing_products:
            print("Missing Products:")
            for product in missing_products:
                print(f"ID: {product['id']}, Title: {product['title']}, Final Price: {product['final_price']}")
        else:
            print("No products are missing in the expected data.")
    
    def compare_final_prices(self):
        '''Compare the final prices of the actual and expected data'''
        expected_prices = {product['id']: product['final_price'] for product in self.expected_data}
        matching_count = 0
        
        for product in self.actual_data:
            expected_price = expected_prices.get(product['id'])
            # If the expected price is not None, round actual price to match its precision
            if expected_price is not None:
                decimal_places = len(str(expected_price).split('.')[1]) if '.' in str(expected_price) else 0
                actual_price = round(product['final_price'], decimal_places)
            else:
                continue
            if abs(actual_price - expected_price) == 0:
                matching_count += 1
        
        print(f"Number of rows where final price matches: {matching_count}")
    
    def run(self):
        self.fetch_data()
        self.process_data()
        self.load_from_parquet()
        self.get_most_expensive_product()
        self.find_missing_products()
        self.compare_final_prices()

if __name__ == "__main__":
    processor = ProductDataProcessor(
        api_url="https://dummyjson.com/products",
        local_path="data/product_prices_calculated.parquet"
    )
    processor.run()
