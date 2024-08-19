# file for loading data from the JSON file and checking all in order
import json
from pprint import pprint


def load_data(filepath: str):
    with open(filepath, 'r') as file:
        json_order_data = json.load(file)
    return json_order_data


def check_consistent_keys(json_data: list) -> None:
    # helper function to ensure the list of json dicts is consistent for when writing
    # to a dataframe
    reference_keys = set(json_data[0].keys())

    for i, item in enumerate(json_data):
        if not isinstance(item, dict):
            print(f"Item at index {i} is not a dict.")
        if set(item.keys()) != reference_keys:
            print(f"Inconsistent keys found at index: {i}: {set(item.keys())}")


if __name__ == "__main__":
    data_dir = r'/Users/philip.papasavvas/Downloads/vmj_int/'
    json_orders = load_data(filepath=f"{data_dir}/task_data.json")
    check_consistent_keys(json_data=json_orders)
    print("*** Preview of first item... ***")
    pprint(json_orders[0])
