import os
import json
from datetime import datetime

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

def process_data(data_dict):
    """
    Processes each dictionary element in the jsonl file. 
    This is where you can add your custom processing logic.
    """
    if not is_valid_date_format(data_dict):
        print(data_dict['source'])
        #print only the id, source, title, date_published, and url
        #print({k: v for k, v in data_dict.items() if k in ["source", "title", "date_published", "url"]})

def process_jsonl_files(data_dir):
    """
    Goes through the data directory, opens every jsonl file sequentially, 
    and processes every element (which is a dictionary) in the jsonl file.
    """
    for filename in os.listdir(data_dir):
        if filename.endswith(".jsonl"):
            filepath = os.path.join(data_dir, filename)
            print(f"Processing {filepath}")
            with open(filepath, "r", encoding='utf-8') as f:
                for line in f:
                    data_dict = json.loads(line)
                    try:
                        process_data(data_dict)
                    except Exception as e:
                        print(f"Error processing {data_dict} in {filename}")
                        print(e)

if __name__ == "__main__":
    process_jsonl_files("data/")
