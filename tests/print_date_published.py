import json
from dateutil.parser import parse

def print_date_published(file_path, n=10):
    with open(file_path, 'r') as file:
        for i, line in enumerate(file):
            if i >= n:
                break
            entry = json.loads(line)
            print(entry.get('date_published'))

def validate_date_format(file_path, keys_to_print):
    with open(file_path, 'r') as file:
        for i, line in enumerate(file):
            entry = json.loads(line)
            date_published = entry.get('date_published')
            try:
                # Try to parse the date_published string into a datetime object
                parse(date_published)
            except ValueError:
                print(f'Row {i}: date_published is NOT in a valid format: {date_published}')
                for key in keys_to_print:
                    print(f'  {key}: {entry.get(key)}')

# replace with your file path
file_path = "data/distill.jsonl"

# list of keys to print when an invalid date format is found
keys_to_print = ['url', 'title', 'id']

# uncomment to print date_published for the first 10 entries
print_date_published(file_path)

# uncomment to validate date_published format for all entries
validate_date_format(file_path, keys_to_print)
