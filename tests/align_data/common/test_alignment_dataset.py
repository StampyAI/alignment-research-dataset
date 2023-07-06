import json
import pytest
import jsonlines
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from align_data.common.alignment_dataset import AlignmentDataset, GdocDataset, DataEntry


@pytest.fixture
def data_entries():
    entries = [
        DataEntry({
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
    entry = DataEntry({})

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


def test_data_entry_id_from_text():
    data = {'key1': 12, 'key2': 312, 'text': 'once upon a time'}
    entry = DataEntry(data)
    entry.add_id()

    assert entry == dict({
        'date_published': None,
        'id': '457c21e0ecabebcb85c12022d481d9f4',
        'source': None,
        'title': None,
        'url': None,
        'summary': [],
        'authors': [],
        }, **data
    )


def test_data_entry_no_text():
    entry = DataEntry({'key1': 12, 'key2': 312})
    with pytest.raises(AssertionError, match='Entry is missing text'):
        entry.add_id()


def test_data_entry_none_text():
    entry = DataEntry({'key1': 12, 'key2': 312, 'text': None})
    with pytest.raises(AssertionError, match='Entry is missing text'):
        entry.add_id()


def test_data_entry_verify_id_passes():
    entry = DataEntry({'text': 'once upon a time', 'id': '457c21e0ecabebcb85c12022d481d9f4'})
    entry._verify_id()


@pytest.mark.parametrize('data, error', (
    ({'text': 'bla bla bla'}, 'Entry is missing id'),
    ({'text': 'bla bla bla', 'id': None}, 'Entry is missing id'),

    ({'id': '123'}, 'Entry is missing text'),
    ({'id': '123', 'text': None}, 'Entry is missing text'),

    ({'id': '123', 'text': 'winter wonderland'}, 'Entry id does not match text'),
    ({'id': '457c21e0ecabebcb85c12022d481d9f4', 'text': 'winter wonderland'}, 'Entry id does not match text'),
    ({'id': '457c21e0ecabebcb85c12022d481d9f4', 'text': 'Once upon a time'}, 'Entry id does not match text'),
))
def test_data_entry_verify_id_fails(data, error):
    entry = DataEntry(data)
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
            return DataEntry({
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
