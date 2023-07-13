import pytest
import datetime
from pathlib import Path
from main import AlignmentDataset
from collections import defaultdict
from align_data import ALL_DATASETS, DATASET_REGISTRY, get_dataset

OUTPATH = Path('tests/tmp_data')

def check_valid_entry(entry):
    # You'll likely want to replace these with the actual field names for your JSON objects.
    fields_and_types = {
        'id': str,
        'title': str,
        'text': str,
        'url': str,
        'date_published': str,
        'source': str,
        'authors': list,
        'summary': list,
    }

    # Check for the right format
    for field, expected_type in fields_and_types.items():
        assert field in entry, f"Missing field {field}"
        actual_type = type(entry[field])
        assert actual_type == expected_type, f"Field {field} expects type {type} but got {type(entry[field])}"
    
    assert datetime.strptime(entry['date_published'], "%Y-%m-%dT%H:%M:%SZ") is not None, f"date_published {entry['date_published']} is not in the correct format, YYYY-MM-DDTHH:MM:SSZ"
    assert len(entry['id']) == 32, f"ID {entry['id']} is not the correct length (32)"
    assert entry['source'] in ALL_DATASETS, f"Source {entry['source']} is not a valid dataset name"

def entry_str(entry):
    return f"\tID: {entry['id']}, title: {entry['title']}, source: {entry['source']}"

@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown():
    print("Setting up tests...")

    OUTPATH.mkdir(parents=True, exist_ok=True)
    alignment_dataset = AlignmentDataset(out_path=OUTPATH)

    # Fetch all datasets at setup
    for dataset in DATASET_REGISTRY:
        print(f"Setting up {dataset.name}")

        dataset._set_output_paths(OUTPATH)

        if dataset.jsonl_path.exists():
            dataset.jsonl_path.unlink()

        print(f"Fetching dataset: {dataset.name}")
        alignment_dataset.fetch(dataset.name)
        assert dataset.jsonl_path.exists()

    yield # Run tests

    # Cleanup after all tests
    print("Cleaning up tests...")

    for dataset in DATASET_REGISTRY:
        print(f"Cleaning up {dataset.name}")

        dataset._set_output_paths(OUTPATH)
        dataset.jsonl_path.unlink(missing_ok=True)
        dataset.txt_path.unlink(missing_ok=True)


@pytest.mark.slow
def test_validate_jsonl_content():
    for dataset in DATASET_REGISTRY:
        dataset._set_output_paths(OUTPATH)
        assert dataset.jsonl_path.exists()

        for entry in dataset.read_entries():
            check_valid_entry(entry)


@pytest.mark.slow
def test_no_duplicate_ids():
    ids_dict = defaultdict(list)

    for dataset in DATASET_REGISTRY:
        dataset._set_output_paths(OUTPATH)
        assert dataset.jsonl_path.exists()

        for entry in dataset.read_entries():
            ids_dict[entry['id']].append(entry)

    for id, entries in ids_dict.items():
        if len(entries) > 1:
            duplicate_sources = '\n'.join([entry_str(entry) for entry in entries])
            pytest.fail(f"Duplicate ID {id} found in sources:\n{duplicate_sources}")
