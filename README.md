## Python self-assessment task 

This Python task involves fetching data from API, JSON data handling, working with parquet files
and performing simple calculations. Also, it requires basic git skills to upload the results. 

### Actual data
Get products data from `https://dummyjson.com/products`.
- **Total amount of products - 194** (use query parameters in the above path)
- Final price for each product can be calculated: `final_price = price * discount`

### Expected data 
Stored in `./data/product_prices_calculated.parquet` 

### Questions
1. What product is the most expensive according to actual data?
2. What product is missing in expected data?
3. For how many rows final price in expected data matches with calculated price from actual data?

### Task
1. Clone this repo
2. Create separate feature branch
3. Write your Python code that answers the above questions
4. Create PR into the master branch
5. Share the PR link with our HR

**Pay attention, that your code should follow PEP8 standards.** 
It should be easy to read, effective and explicit.


If you have questions, feel free to reach out - dbulychev@jaxel.com

# Steps to Run the Assessment Task

## Create Virtual Environment
```sh
virtualenv -p python3.12 venv
```

## Activate Virtual Environment
```sh
source venv/bin/activate
```

## Install python dependancies
```sh
pip install -r requirements.txt
```

## Run python script and get the results
```sh
python main.py
```