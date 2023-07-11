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

def validate_data(data_dict, seen_urls):
    """
    Processes each dictionary element in the jsonl file. 
    """
    if not is_valid_date_format(data_dict):
        raise ValueError(f"Invalid date format for source: {data_dict['source']}, title: {data_dict['title'][:30]}, date_pub: {data_dict['date_published']}")
    # TODO: add more checks here

def check_for_duplicates(data_dict, seen_urls):
    id = data_dict.get('id')
    if id in seen_urls:
        #raise ValueError(f"Duplicate id found. \nId: {id}\nfirst_duplicate: {get_data_dict_str(seen_urls[id])}\nsecond_duplicate: {get_data_dict_str(data_dict)}\n\n\n\n")
        seen_urls[id].append(data_dict)
    else:
        seen_urls[id] = [data_dict]

    #TODO: Add more validation logic here
    return seen_urls 

def get_data_dict_str(data_dict):
    """
    Returns a string representation of the given data_dict.
    """
    return f"source: {data_dict['source']}, title: {data_dict['title'][:50]}, date_pub: {data_dict['date_published']}, url: {data_dict['url']}\n"

def files_iterator(data_dir):
    """
    Goes through the data directory, opens every jsonl file sequentially, 
    and yields every element (which is a dictionary) in the jsonl file.
    """
    for path in Path(data_dir).glob('*.jsonl'):
        with open(path, encoding='utf-8') as f:
            for line in f:
                yield json.loads(line)

def process_jsonl_files(data_dir):
    seen_urls = dict()  # holds all seen urls
    for data_dict in files_iterator(data_dir):
        try:
            validate_data(data_dict, seen_urls)
            seen_urls = check_for_duplicates(data_dict, seen_urls)
        except ValueError as e:
            print(e)
    for id, duplicates in seen_urls.items():
        if len(duplicates) > 1:
            print(f"{len(duplicates)} duplicate ids found. \nId: {id}\n{''.join([f'id {i+1}: {get_data_dict_str(duplicate)}' for i, duplicate in enumerate(duplicates)])}\n\n\n\n")

def delete_all_txt_and_jsonl(data_dir):
    """
    Deletes all txt and jsonl files in the given directory.
    """
    for path in Path(data_dir).glob('*.txt'):
        os.remove(path)
    for path in Path(data_dir).glob('*.jsonl'):
        os.remove(path)

if __name__ == "__main__":
    process_jsonl_files("data/")
    #delete_all_txt_and_jsonl("data/")
