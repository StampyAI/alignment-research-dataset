import os
import json
from datetime import datetime
from pathlib import Path

def is_valid_date_format(data_dict, format="%Y-%m-%dT%H:%M:%SZ"):
    """
    Checks if the given date string matches the expected format.
    """
    try:
        date_str = data_dict.get("date_published")
        datetime.strptime(date_str, format)
        return True
    except ValueError:
        return False

def validate_data(data_dict):
    """
    Processes each dictionary element in the jsonl file. 
    """
    if not is_valid_date_format(data_dict):
        raise ValueError(f"Invalid date format for source: {data_dict['source']}, title: {data_dict['title'][:30]}, date_pub: {data_dict['date_published']}")
    
    #TODO: Add more validation logic here

def process_jsonl_files(data_dir):
    """
    Goes through the data directory, opens every jsonl file sequentially, 
    and processes every element (which is a dictionary) in the jsonl file.
    """
    for path in Path(data_dir).glob('*.jsonl'):
        with open(path, encoding='utf-8') as f:
            for line in f:
                data_dict = json.loads(line)
                try:
                    validate_data(data_dict)
                except ValueError as e:
                    print(e)

if __name__ == "__main__":
    process_jsonl_files("data/")
