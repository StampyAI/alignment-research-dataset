import pytest
from align_data.db.models import Article
import jsonlines
from unittest.mock import patch
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import List
from align_data.common.alignment_dataset import AlignmentDataset


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
        Article.before_write(None, None, entry)
    return entries


@pytest.fixture
def dataset(tmp_path):
    dataset = AlignmentDataset(name='blaa')
    dataset.__post_init__(Path(tmp_path))
    return dataset


def test_data_entry_default_fields():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({})

    assert entry.to_dict() == {
        'date_published': None,
        'source': None,
        'source_type': None,
        'title': None,
        'url': None,
        'id': None,
        'text': None,
        'summaries': [],
        'authors': [],
    }

def test_data_entry_id_from_urls_and_title():
    data = {'key1': 12, 'key2': 312, 'url': 'www.arbital.org', 'title': 'once upon a time'}
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry(data)
    Article.before_write(None, None, entry)
    assert entry.to_dict() == dict({
        'date_published': None,
        'id': '770fe57c8c2130eda08dc392b8696f97',
        'source': None,
        'source_type': None,
        'text': None,
        'summaries': [],
        'authors': [],
        }, **data
    )


def test_data_entry_no_url_and_title():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url', 'title'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_no_url():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'title': 'wikipedia goes to war on porcupines'})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_none_url():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': None})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url', 'title'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_none_title():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': 'www.wikipedia.org', 'title': None})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['title'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_empty_url_and_title():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': '', 'title': ''})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url', 'title'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_empty_url_only():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': '', 'title': 'once upon a time'})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_empty_title_only():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'key1': 12, 'key2': 312, 'url': 'www.wikipedia.org', 'title':''})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['title'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_verify_id_passes():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'source': 'arbital', 'text': 'once upon a time', 'url': 'www.arbital.org', 'title': 'once upon a time', 'id': '770fe57c8c2130eda08dc392b8696f97'})
    entry.verify_id()


def test_data_entry_verify_id_fails():
    dataset = AlignmentDataset(name='blaa')
    entry = dataset.make_data_entry({'url': 'www.arbital.org', 'title': 'once upon a time', 'id': 'f2b4e02fc1dd8ae43845e4f930f2d84f'})
    with pytest.raises(AssertionError, match='Entry id does not match id_fields'):
        entry.verify_id()


def test_data_entry_id_fields_url_no_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'source': 'arbital', 'text': 'once upon a time'})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_id_fields_url_empty_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'url': ''})
    with pytest.raises(AssertionError, match="Entry is missing the following fields: \\['url'\\]"):
        Article.before_write(None, None, entry)


def test_data_entry_id_fields_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'url': 'https://www.google.ca/once_upon_a_time'})

    Article.before_write(None, None, entry)
    assert entry.id


def test_data_entry_id_fields_url_verify_id_passes():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry = dataset.make_data_entry({'url': 'arbitalonce upon a time', 'id':'809d336a0b9b38c4f585e862317e667d'})
    entry.verify_id()


def test_data_entry_different_id_from_different_url():
    dataset = AlignmentDataset(name='blaa', id_fields=['url'])
    entry1 = dataset.make_data_entry({'url': ' https://aisafety.info?state=6478'})
    entry2 = dataset.make_data_entry({'source': 'arbital', 'text': 'once upon a time', 'url': ' https://aisafety.info?state=6479'})
    assert entry1.generate_id_string() != entry2.generate_id_string()


@pytest.mark.parametrize('data, error', (
    ({'text': 'bla bla bla'}, "Entry is missing id"),
    ({'text': 'bla bla bla', 'id': None}, "Entry is missing id"),

    ({'id': '123', 'url':'www.google.com/winter_wonderland','title': 'winter wonderland'}, 'Entry id 123 does not match id from id_fields, [0-9a-fA-F]{32}'),
    ({'id': '457c21e0ecabebcb85c12022d481d9f4', 'url':'www.google.com', 'title': 'winter wonderland'}, 'Entry id [0-9a-fA-F]{32} does not match id from id_fields, [0-9a-fA-F]{32}'),
    ({'id': '457c21e0ecabebcb85c12022d481d9f4', 'url':'www.google.com', 'title': 'Once upon a time'}, 'Entry id [0-9a-fA-F]{32} does not match id from id_fields, [0-9a-fA-F]{32}'),
))
def test_data_entry_verify_id_fails(data, error):
    dataset = AlignmentDataset(name='blaa', id_fields=['url', 'title'])
    entry = dataset.make_data_entry(data)
    with pytest.raises(AssertionError, match=error):
        entry.verify_id()


@pytest.mark.parametrize('data, error', (
    ({'id': '123'}, "Entry is missing the following fields: \\['url', 'title'\\]"),
    ({'id': '123', 'url': None}, "Entry is missing the following fields: \\['url', 'title'\\]"),
    ({'id': '123', 'url': 'www.google.com/'}, "Entry is missing the following fields: \\['title'\\]"),
    ({'id': '123', 'url': 'google', 'text': None}, "Entry is missing the following fields: \\['title'\\]"),
    ({'id': '123', 'url': '', 'title': ''}, "Entry is missing the following fields: \\['url', 'title'\\]"),
))
def test_data_entry_verify_fields_fails(data, error):
    dataset = AlignmentDataset(name='blaa', id_fields=['url', 'title'])
    entry = dataset.make_data_entry(data)
    with pytest.raises(AssertionError, match=error):
        entry.verify_fields()


def test_alignment_dataset_default_values(dataset, tmp_path):
    assert dataset.name == 'blaa'

    # Make sure all data paths are correct
    assert dataset.data_path.resolve() == tmp_path.resolve()
    assert dataset.raw_data_path.resolve() == (tmp_path / 'raw').resolve()
    assert dataset.files_path.resolve() == (tmp_path / 'raw' / dataset.name).resolve()

    # Make sure the output files are correct
    assert dataset.jsonl_path.resolve() == (tmp_path / f'{dataset.name}.jsonl').resolve()


def test_alignment_dataset_file_list(dataset, tmp_path):
    dataset.glob = '*.bla'
    dataset.files_path = tmp_path

    for i in range(5):
        (Path(tmp_path) / f'test{i}.bla').touch()

    for i in range(5, 10):
        (Path(tmp_path) / f'test{i}.txt').touch()

    files = [f.resolve() for f in dataset.items_list]
    assert files == list(Path(tmp_path).glob('*bla'))


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
                'authors': [],
            })

    dataset = NumbersDataset(name='numbers', nums=list(range(10)))
    dataset.__post_init__(data_path=tmp_path)
    return dataset


def test_unprocessed_items_fresh(numbers_dataset):
    """Getting the unprocessed items from a dataset that hasn't written anything should get all items."""
    seen = set()
    with patch.object(numbers_dataset, '_load_outputted_items', return_value=seen):
        assert list(numbers_dataset.unprocessed_items()) == list(range(10))


def test_unprocessed_items_all_done(numbers_dataset):
    """Getting the unprocessed items from a dataset that has already processed everything should return an empty list."""
    seen = set(range(0, 10))
    with patch.object(numbers_dataset, '_load_outputted_items', return_value=seen):
        assert list(numbers_dataset.unprocessed_items()) == []


def test_unprocessed_items_some_done(numbers_dataset):
    """Getting the uprocessed items from a dataset that has partially completed should return the items that haven't been processed."""
    seen = set(range(0, 10, 2))
    with patch.object(numbers_dataset, '_load_outputted_items', return_value=seen):
        assert list(numbers_dataset.unprocessed_items()) == list(range(1, 10, 2))


def test_fetch_entries(numbers_dataset):
    assert [i.meta['value'] for i in numbers_dataset.fetch_entries()] == [i**2 for i in range(10)]


def test_format_datatime(dataset):
    assert dataset._format_datetime(datetime(2022, 1, 1, 12, 23, 43)) == '2022-01-01T12:23:43Z'


def test_format_datatime_ignore_timezone(dataset):
    dt = datetime.fromisoformat('2022-01-01T00:00:00+04:00')
    assert dataset._format_datetime(dt) == '2022-01-01T00:00:00Z'
