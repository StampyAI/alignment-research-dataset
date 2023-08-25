import pytest
from unittest.mock import patch

from align_data.sources.airtable import AirtableDataset


@pytest.mark.parametrize('item, overwrites', (
    ({'url': 'http://bla.vle'}, {}),
    ({'url': 'http://bla.vle', 'source': 'your momma'}, {'source': 'your momma'}),
    ({'url': 'http://bla.vle', 'source': 'your momma', 'bla': 'ble'}, {'source': 'your momma'}),
    (
        {'url': 'http://bla.vle', 'status': 'fine', 'title': 'Something or other'},
        {'status': 'fine', 'title': 'Something or other'}
    ),
    (
        {'url': 'http://some.other.url', 'source_type': 'blog', 'authors': 'bla, bla, bla'},
        {'url': 'http://some.other.url', 'source_type': 'blog', 'authors': 'bla, bla, bla'}
    ),
))
def test_map_cols_no_mapping(item, overwrites):
    dataset = AirtableDataset(name='asd', base_id='ddwe', table_id='csdcsc', mappings={}, processors={})
    assert dataset.map_cols({'id': '123', 'fields': item}) == dict({
        'authors': None,
        'comments': None,
        'date_published': None,
        'id': None,
        'source': None,
        'source_type': None,
        'status': None,
        'text': None,
        'title': None,
        'summary': None,
        'url': 'http://bla.vle'
    }, **overwrites)


@pytest.mark.parametrize('item, overwrites', (
    ({'an url!': 'http://bla.vle'}, {}),
    ({'an url!': 'http://bla.vle', 'source': 'your momma'}, {'source': 'your momma'}),
    ({'an url!': 'http://bla.vle', 'source': 'your momma', 'bla': 'ble'}, {'source': 'your momma'}),
    (
        {'an url!': 'http://bla.vle', 'status': 'fine', 'title': 'Something or other'},
        {'status': 'fine', 'title': 'Something or other'}
    ),
    (
        {'an url!': 'http://some.other.url', 'source_type': 'blog', 'whodunnit': 'bla, bla, bla'},
        {'url': 'http://some.other.url', 'source_type': 'blog', 'authors': 'bla, bla, bla'}
    ),
))
def test_map_cols_with_mapping(item, overwrites):
    dataset = AirtableDataset(
        name='asd', base_id='ddwe', table_id='csdcsc',
        mappings={
            'url': 'an url!',
            'authors': 'whodunnit',
        },
        processors={}
    )
    assert dataset.map_cols({'id': '123', 'fields': item}) == dict({
        'authors': None,
        'comments': None,
        'date_published': None,
        'id': None,
        'source': None,
        'source_type': None,
        'status': None,
        'text': None,
        'title': None,
        'summary': None,
        'url': 'http://bla.vle'
    }, **overwrites)


@pytest.mark.parametrize('item, overwrites', (
    ({'an url!': 'http://bla.vle'}, {}),
    ({'an url!': 'http://bla.vle', 'source': 'your momma'}, {'source': 'your momma'}),
    ({'an url!': 'http://bla.vle', 'source': 'your momma', 'bla': 'ble'}, {'source': 'your momma'}),
    (
        {'an url!': 'http://bla.vle', 'status': 'fine', 'title': 'Something or other'},
        {'status': 'fine', 'title': 'Something or other bla!'}
    ),
    (
        {'an url!': 'http://some.other.url', 'source_type': 'blog', 'whodunnit': 'bla, bla, bla'},
        {'url': 'http://some.other.url', 'source_type': 'blog', 'authors': 'bla, bla, bla'}
    ),
))
def test_map_cols_with_processing(item, overwrites):
    dataset = AirtableDataset(
        name='asd', base_id='ddwe', table_id='csdcsc',
        mappings={
            'url': 'an url!',
            'authors': 'whodunnit',
        },
        processors={
            'title': lambda val: val and val + ' bla!',
            'id': lambda _: 123,
        }
    )
    assert dataset.map_cols({'id': '123', 'fields': item}) == dict({
        'authors': None,
        'comments': None,
        'date_published': None,
        'id': 123,
        'source': None,
        'source_type': None,
        'status': None,
        'text': None,
        'title': None,
        'summary': None,
        'url': 'http://bla.vle'
    }, **overwrites)


@pytest.mark.parametrize('url', (None, '', 'asdasdsad'))
def test_map_cols_no_url(url):
    dataset = AirtableDataset(name='asd', base_id='ddwe', table_id='csdcsc', mappings={}, processors={})
    assert dataset.map_cols({'id': '123', 'fields': {'url': url}}) is None


def test_process_entry():
    dataset = AirtableDataset(name='asd', base_id='ddwe', table_id='csdcsc', mappings={}, processors={})
    entry = {
        'url': 'http://bla.cle',
        'authors': ['johnny', 'your momma', 'mr. Blobby', 'Łóżćś Jaś'],
        'date_published': '2023-01-02',
        'source': 'some place',
        'status': 'fine',
        'comments': 'should be ok',
    }
    with patch("align_data.sources.airtable.item_metadata", return_value={
        'text': 'bla bla bla',
        'source_type': 'some kind of thing',
    }):
        assert dataset.process_entry(entry).to_dict() == {
            'authors': ['johnny', 'your momma', 'mr. Blobby', 'Łóżćś Jaś'],
            'date_published': '2023-01-02T00:00:00Z',
            'id': None,
            'source': 'asd',
            'source_type': 'some kind of thing',
            'summaries': [],
            'text': 'bla bla bla',
            'title': '',
            'url': 'http://bla.cle',
        }
