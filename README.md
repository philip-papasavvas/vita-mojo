# VitaMojo Lead Data Engineer Task Submission
This repository contains the submission from Philip Papasavvas for the VitaMojo Lead Data Engineer Task.

### Project Structure
The project is organized into the following files:

- data_loading.py: Helper functions for loading the (unzipped) JSON data and a function to check the consistency of the data. Includes the functions: 
  `load_data` and `check_consistent_keys` 
- data_preprocessing.py: This file contains functions for preprocessing the JSON data and 
  preparing it for insertion into the database. It includes the following functions:
`return_tenant_store_mapping`, `return_detailed_order_information_dataframe`, `return_order_information_dataframe`
- main.py: This file serves as the main script that should be considered for the submission. 
  - The first part of the script contains data loading, preprocessing, database creation, 
  and querying. 
  - The script then answers the questions in the take home task regarding
1. Total number of orders
2. Number of orders from each channel
3. Top 5 items sold for each tenant
4. Items sold more than 5 times for each tenant

## Usage
To run the project, follow these steps:
- Ensure that you have Python 3.12 installed on your machine, and poetry (package manager).
- If you don't have poetry installed, install it by running:
> curl -sSL https://install.python-poetry.org | python3 -
Install the project dependencies
- poetry install
- Ensure that you have the (unzipped) JSON data in the `data_dir` which is specified
in the `main.py` 
