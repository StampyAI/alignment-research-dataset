import json
import re
import pytest
import jsonlines
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from align_data.common.alignment_dataset import AlignmentDataset, GdocDataset


@pytest.fixture
def data_entries():
    dataset = AlignmentDataset(name='blaa')
    entries = [
        dataset.make_data_entry({
            'text': f'line {i}',
            'date_published': f'day {i}',
            'source': f'source {i}',
            'title': str(i),
            'url': f'http://bla.bla.bla?page={i}',
        }) for i in range(5)
    ]
    for entry in entries:
        entry.add_id()
    return entries


@pytest.fixture
def dataset(tmp_path):
    dataset = AlignmentDataset(name='blaa')
    dataset.__post_init__(Path(tmp_path))
    return dataset


def test_data_entry_default_fields():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({})

    assert entry == {
        'date_published': None,
        'source': None,
        'title': None,
        'url': None,
        'id': None,
        'text': None,
        'summary': [],
        'authors': [],
    } 

def test_data_entry_id_from_urls_and_title():
    data = {'key1': 12, 'key2': 312, 'url': 'www.arbital.org', 'title': 'once upon a time'}
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry(data)
    entry.add_id()
    print(entry)
    assert entry == dict({
        'date_published': None,
        'id': '770fe57c8c2130eda08dc392b8696f97',
        'source': None,
        'text': None,
        'summary': [],
        'authors': [],
        }, **data
    )


def test_data_entry_no_url_and_title():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url', 'title'\\]"):
        entry.add_id()

def test_data_entry_no_url():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'title': 'wikipedia goes to war on porcupines'})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        entry.add_id()

def test_data_entry_none_url():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': None})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url', 'title'\\]"):
        entry.add_id()

def test_data_entry_none_title():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': 'www.wikipedia.org', 'title': None})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['title'\\]"):
        entry.add_id()
    
def test_data_entry_empty_url_and_title():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': '', 'title': ''})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url', 'title'\\]"):
        entry.add_id()

def test_data_entry_empty_url_only():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': '', 'title': 'once upon a time'})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        entry.add_id()

def test_data_entry_empty_title_only():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': 'www.wikipedia.org', 'title':''})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['title'\\]"):
        entry.add_id()

def test_data_entry_verify_id_passes():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'source': 'arbital', 'text': 'once upon a time', 'url': 'www.arbital.org', 'title': 'once upon a time', 'id': '770fe57c8c2130eda08dc392b8696f97'})
    entry._verify_id()

def test_data_entry_verify_id_fails():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'url': 'www.arbital.org', 'title': 'once upon a time', 'id': 'f2b4e02fc1dd8ae43845e4f930f2d84f'})
    with pytest.raises(AssertionError, match='Entry id does not match id_fields'):
        entry._verify_id()

def test_data_entry_id_fields_url_no_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'source': 'arbital', 'text': 'once upon a time'})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        entry.add_id()

def test_data_entry_id_fields_url_empty_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'url': ''})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        entry.add_id()
    
def test_data_entry_id_fields_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'url': 'https://www.google.ca/once_upon_a_time'})
    entry.add_id()

def test_data_entry_id_fields_url_verify_id_passes():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'url': 'arbitalonce upon a time', 'id':'809d336a0b9b38c4f585e862317e667d'})
    entry._verify_id()

def test_data_entry_different_id_from_different_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry1 = dataset.make_data_entry({'url': ' https://aisafety.info?state=6478'})
    entry1.add_id()
    entry2 = dataset.make_data_entry({'source': 'arbital', 'text': 'once upon a time', 'url': ' https://aisafety.info?state=6479'})
    entry2.add_id()
    assert entry1['id'] != entry2['id']


@pytest.mark.parametrize('data, error', (
    ({'text': 'bla bla bla'}, "Entry is missing id"),
    ({'text': 'bla bla bla', 'id': None}, "Entry is missing id"),

    ({'id': '123'}, "Entry is missing the following fields: \\['url', 'title'\\]"),
    ({'id': '123', 'url': None}, "Entry is missing the following fields: \\['url', 'title'\\]"),
    ({'id': '123', 'url': 'www.google.com/'}, "Entry is missing the following fields: \\['title'\\]"),
    ({'id': '123', 'url': 'google', 'text': None}, "Entry is missing the following fields: \\['title'\\]"),
    ({'id': '123', 'url': '', 'title': ''}, "Entry is missing the following fields: \\['url', 'title'\\]"),

    ({'id': '123', 'url':'www.google.com/winter_wonderland','title': 'winter wonderland'}, 'Entry id 123 does not match id from id_fields, [0-9a-fA-F]{32}'),
    ({'id': '457c21e0ecabebcb85c12022d481d9f4', 'url':'www.google.com', 'title': 'winter wonderland'}, 'Entry id [0-9a-fA-F]{32} does not match id from id_fields, [0-9a-fA-F]{32}'),
    ({'id': '457c21e0ecabebcb85c12022d481d9f4', 'url':'www.google.com', 'title': 'Once upon a time'}, 'Entry id [0-9a-fA-F]{32} does not match id from id_fields, [0-9a-fA-F]{32}'),
))
def test_data_entry_verify_id_fails(data, error):
    dataset = AlignmentDataset(name='blaa', id_fields=['url', 'title'])
    entry = dataset.make_data_entry(data)
    with pytest.raises(AssertionError, match=error):
        entry._verify_id()


def test_alignment_dataset_default_values(dataset, tmp_path):
    assert dataset.name == 'blaa'

    # Make sure all data paths are correct
    assert dataset.data_path.resolve() == tmp_path.resolve()
    assert dataset.raw_data_path.resolve() == (tmp_path / 'raw').resolve()
    assert dataset.files_path.resolve() == (tmp_path / 'raw' / dataset.name).resolve()

    # Make sure the output files are correct
    assert dataset.jsonl_path.resolve() == (tmp_path / f'{dataset.name}.jsonl').resolve()
    assert dataset.txt_path.resolve() == (tmp_path / f'{dataset.name}.txt').resolve()


def test_alignment_dataset_file_list(dataset, tmp_path):
    dataset.glob = '*.bla'
    dataset.files_path = tmp_path

    for i in range(5):
        (Path(tmp_path) / f'test{i}.bla').touch()

    for i in range(5, 10):
        (Path(tmp_path) / f'test{i}.txt').touch()

    files = [f.resolve() for f in dataset.items_list]
    assert files == list(Path(tmp_path).glob('*bla'))


def check_written_files(output_path, name, entries):
    with jsonlines.open(Path(output_path) / f'{name}.jsonl', mode='r') as reader:
        assert list(reader) == entries, f'Not all entries were output to the {name}.jsonl file'

    with open(Path(output_path) / f'{name}.txt') as f:
        assert len(f.readlines()) == len(entries) * 3, f'Not all entries were output to the {name}.txt file'

    return True


def test_alignment_dataset_writer_default_paths(dataset, tmp_path, data_entries):
    with dataset.writer() as writer:
        for entry in data_entries:
            writer(entry)

    assert check_written_files(tmp_path, dataset.name, data_entries)


def test_alignment_dataset_writer_provided_paths(dataset, tmp_path, data_entries):
    with dataset.writer(out_path=tmp_path) as writer:
        for entry in data_entries:
            writer(entry)

    assert check_written_files(tmp_path, dataset.name, data_entries)


def test_alignment_dataset_writer_append(dataset, tmp_path, data_entries):
    with dataset.writer() as writer:
        for entry in data_entries:
            writer(entry)

    with dataset.writer(overwrite=False) as writer:
        for entry in data_entries:
            writer(entry)

    assert check_written_files(tmp_path, dataset.name, data_entries * 2)


def test_alignment_dataset_writer_overwrite(dataset, tmp_path, data_entries):
    with dataset.writer() as writer:
        for entry in data_entries:
            writer(entry)

    with dataset.writer(overwrite=True) as writer:
        for entry in data_entries:
            writer(entry)

    assert check_written_files(tmp_path, dataset.name, data_entries)


def test_read_entries(dataset, tmp_path, data_entries):
    with dataset.writer() as writer:
        for entry in data_entries:
            writer(entry)

    assert list(dataset.read_entries()) == data_entries


def test_merge_summaries_no_key(dataset):
    dataset.summary_key = None

    assert dataset.merge_summaries({}) is None


def test_merge_summaries_no_file(dataset):
    assert dataset.merge_summaries({}) is None


def test_merge_summaries(dataset, data_entries):
    dataset.summary_key = 'summary'
    with dataset.writer() as writer:
        for entry in data_entries:
            writer(entry)

    dataset.merge_summaries({
        'http://bla.bla.bla?page=1': {
            'source1': 'This should be the first summary',
            'source2': 'This should be the second one'
        },
        'http://bla.bla.bla?page=3': {
            'source': 'This should be the only one'
        },
    })

    data_entries[1]['summary'] = ['This should be the first summary', 'This should be the second one']
    data_entries[3]['summary'] = ['This should be the only one']
    assert data_entries == list(dataset.read_entries())


@pytest.fixture
def numbers_dataset(tmp_path):
    """Make a dataset that raises its items to the power of 2."""
    @dataclass
    class NumbersDataset(AlignmentDataset):
        nums: List[int]
        done_key = 'number'

        @property
        def items_list(self):
            return self.nums

        def get_item_key(self, item):
            return item

        def process_entry(self, item):
            return self.make_data_entry({
                'text': f'line {item}',
                'date_published': f'day {item}',
                'source': f'source {item}',
                'title': str(item),
                'url': f'http://bla.bla.bla?page={item}',
                'number': item,
                'value': item ** 2,
            })

    dataset = NumbersDataset(name='numbers', nums=list(range(10)))
    dataset.__post_init__(data_path=tmp_path)
    return dataset


def test_unprocessed_items_fresh(numbers_dataset):
    """Getting the unprocessed items from a dataset that hasn't written anything should get all items."""
    assert list(numbers_dataset.unprocessed_items()) == list(range(10))


def test_unprocessed_items_all_done(numbers_dataset):
    """Getting the unprocessed items from a dataset that has already processed everything should return an empty list."""
    with numbers_dataset.writer() as writer:
        for i in range(10):
            entry = numbers_dataset.process_entry(i)
            entry.add_id()
            writer(entry)

    assert list(numbers_dataset.unprocessed_items()) == []


def test_unprocessed_items_some_done(numbers_dataset):
    """Getting the uprocessed items from a dataset that has partially completed should return the items that haven't been processed."""
    with numbers_dataset.writer() as writer:
        for i in range(0, 10, 2):
            entry = numbers_dataset.process_entry(i)
            entry.add_id()
            writer(entry)

    assert list(numbers_dataset.unprocessed_items()) == list(range(1, 10, 2))


def test_fetch_entries(numbers_dataset):
    assert [i['value'] for i in numbers_dataset.fetch_entries()] == [i**2 for i in range(10)]


def test_format_datatime(dataset):
    assert dataset._format_datetime(datetime(2022, 1, 1, 12, 23, 43)) == '2022-01-01T12:23:43Z'


def test_format_datatime_ignore_timezone(dataset):
    dt = datetime.fromisoformat('2022-01-01T00:00:00+04:00')
    assert dataset._format_datetime(dt) == '2022-01-01T00:00:00Z'
