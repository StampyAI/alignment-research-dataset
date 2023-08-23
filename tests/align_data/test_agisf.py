import pytest
from unittest.mock import patch

from align_data.sources.agisf.agisf import AGISFPodcastDataset


SAMPLE_ITEM = {
    'title': '[Week 0] “Machine Learning for Humans, Part 2.1: Supervised Learning” by Vishal Maini',
    'content': 'this is needed, but will mostly be ignored',
    'summary': '<p>Bla bla bla</p><br /><br /><b>Original article:<br /></b><a href="https://medium.com/machine-learning-for-humans/supervised-learning-740383a2feab">https://medium.com/machine-learning-for-humans/supervised-learning-740383a2feab</a><br /><br /><b>Author:<br /></b>Vishal Maini</p>',
    'link': 'https://ble.ble.com',
}


def test_fetch_contents():
    dataset = AGISFPodcastDataset(name='bla', url='https://bla.bla.com')
    url = 'https://test.url'
    dataset.items = {url: SAMPLE_ITEM}
    assert dataset.fetch_contents(url) == dict(
        SAMPLE_ITEM, authors='Vishal Maini',
        title='Machine Learning for Humans, Part 2.1: Supervised Learning'
    )


def test_fetch_contents_bad_title():
    dataset = AGISFPodcastDataset(name='bla', url='https://bla.bla.com')
    url = 'https://test.url'
    dataset.items = {url: dict(SAMPLE_ITEM, title='asdasdasd')}
    assert dataset.fetch_contents(url) == dict(SAMPLE_ITEM, title='asdasdasd')


def test_get_text():
    dataset = AGISFPodcastDataset(name='bla', url='https://bla.bla.com')
    item = dict(SAMPLE_ITEM)

    with patch("align_data.sources.agisf.agisf.item_metadata", return_value={
        'text': 'bla bla bla',
        'source_type': 'some kind of thing',
        'title': None,
            'authors': [],
            'content': 'this should now change',
    }):
        assert dataset._get_text(item) == 'bla bla bla'
        assert item == dict(
            SAMPLE_ITEM,
            content='this should now change',
            text='bla bla bla',
            source_type='some kind of thing',
        )


@pytest.mark.parametrize('authors, expected', (
    (None, ['default']),
    ('', ['default']),
    ([], ['default']),

    ('bla', ['bla']),
    ('johnny bravo,    mr. blobby\t\t\t, Hans Klos   ', ['johnny bravo', 'mr. blobby', 'Hans Klos']),
    (['mr. bean'], ['mr. bean']),
    (['johnny bravo', 'mr. blobby', 'Hans Klos'], ['johnny bravo', 'mr. blobby', 'Hans Klos']),
))
def test_extract_authors(authors, expected):
    dataset = AGISFPodcastDataset(name='bla', url='https://bla.bla.com', authors=['default'])
    item = dict(SAMPLE_ITEM, authors=authors)
    assert dataset.extract_authors(item) == expected


def test_extra_values():
    dataset = AGISFPodcastDataset(name='bla', url='https://bla.bla.com', authors=['default'])
    assert dataset._extra_values(SAMPLE_ITEM) == {
        'summary': 'Bla bla bla',
    }


def test_extra_values_no_summary():
    dataset = AGISFPodcastDataset(name='bla', url='https://bla.bla.com', authors=['default'])
    assert dataset._extra_values({}) == {}


def test_process_entry():
    dataset = AGISFPodcastDataset(name='bla', url='https://bla.bla.com')
    url = 'https://test.url'
    dataset.items = {url: SAMPLE_ITEM}

    with patch("align_data.sources.agisf.agisf.item_metadata", return_value={'text': 'bla'}):
        assert dataset.process_entry(url).to_dict() == {
            'authors': ['Vishal Maini'],
            'date_published': None,
            'id': None,
            'source': 'bla',
            'source_type': 'blog',
            'summaries': ['Bla bla bla'],
            'text': 'bla',
            'title': 'Machine Learning for Humans, Part 2.1: Supervised Learning',
            'url': 'https://test.url',
        }
