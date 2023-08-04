from datetime import datetime
from unittest.mock import patch, Mock
import pytest
from align_data.sources.youtube.youtube import YouTubeDataset, YouTubeChannelDataset, YouTubePlaylistDataset
from youtube_transcript_api._errors import NoTranscriptFound, VideoUnavailable, TranscriptsDisabled


@pytest.fixture
def transcriber():
    transcriber = Mock()
    transcriber.list_transcripts.return_value.find_transcript.return_value.fetch.return_value = [
        {'text': 'bla bla'},
        {'text': 'second line'},
        {'text': 'ble ble'},
    ]

    with patch('align_data.sources.youtube.youtube.YouTubeTranscriptApi', transcriber):
        yield


def test_key_required():
    dataset = YouTubeDataset(name='asd')
    with pytest.raises(ValueError, match="No YOUTUBE_API_KEY provided"):
        dataset.setup()


def test_next_page_empty_by_default():
    dataset = YouTubeDataset(name='asd')
    assert not dataset.next_page('collection id', 'token')['items']


@pytest.mark.parametrize('item', (
    {
        'kind': 'youtube#searchResult',
        'id': {
            'kind': 'youtube#video',
            'videoId': 'your_video_id'
        }
    },
    {
        'kind': 'youtube#playlistItem',
        'snippet': {
            'resourceId': {
                'kind': 'youtube#video',
                'videoId': 'your_video_id'
            }
        }
    }
))
def test_get_id_with_id(item):
    dataset = YouTubeDataset(name="bla")
    result = dataset._get_id(item)
    assert result == 'your_video_id'


@pytest.mark.parametrize('item', (
    {'bla': 'bla'},
    {
        'kind': 'invalid_kind',
        'id': {
            'kind': 'youtube#video',
            'videoId': 'your_video_id'
        }
    },
    {
        'kind': 'youtube#searchResult',
        'id': {
            'kind': 'bla bla bla',
            'videoId': 'your_video_id'
        }
    },
    {
        'kind': 'youtube#playlistItem',
        'snippet': {
            'resourceId': {
                'kind': 'invalid_kind',
                'videoId': 'your_video_id'
            }
        }
    }
))
def test_get_id_with_invalid_id(item):
    dataset = YouTubeDataset(name="bla")
    result = dataset._get_id(item)
    assert result is None


def test_fetch_videos_default():
    dataset = YouTubeDataset(name="bla")
    assert list(dataset.fetch_videos('collection')) == []


def test_fetch_videos_with_next_page_token():
    dataset = YouTubeDataset(name="bla")

    items = [
        {
            'items': [{'kind': 'youtube#searchResult', 'id': {'kind': 'youtube#video', 'videoId': str(i)}} for i in range(0, 3)],
            'nextPageToken': "1"
        }, {
            'items': [{'kind': 'youtube#searchResult', 'id': {'kind': 'youtube#video', 'videoId': str(i)}} for i in range(3, 6)],
            'nextPageToken': "2"
        }, {
            'items': [{'kind': 'youtube#searchResult', 'id': {'kind': 'youtube#video', 'videoId': str(i)}} for i in range(6, 9)],
            'nextPageToken': None
        },
    ]

    with patch.object(dataset, 'next_page', side_effect=items):
        assert list(dataset.fetch_videos('collection_id')) == [
            {'id': {'kind': 'youtube#video', 'videoId': str(i)}, 'kind': 'youtube#searchResult'}
            for i in range(9)
        ]


def test_fetch_videos_stops_when_no_next_page_token():
    dataset = YouTubeDataset(name="bla")

    items = [
        {
            'items': [{'kind': 'youtube#searchResult', 'id': {'kind': 'youtube#video', 'videoId': str(i)}} for i in range(0, 3)],
            'nextPageToken': None
        }, {
            'items': [{'kind': 'youtube#searchResult', 'id': {'kind': 'youtube#video', 'videoId': str(i)}} for i in range(3, 6)],
            'nextPageToken': "2"
        }, {
            'items': [{'kind': 'youtube#searchResult', 'id': {'kind': 'youtube#video', 'videoId': str(i)}} for i in range(6, 9)],
            'nextPageToken': "3"
        },
    ]

    with patch.object(dataset, 'next_page', side_effect=items):
        assert list(dataset.fetch_videos('collection_id')) == [
            {'id': {'kind': 'youtube#video', 'videoId': str(i)}, 'kind': 'youtube#searchResult'}
            for i in range(3)
        ]


def test_items_list():
    dataset = YouTubeDataset(name="bla")
    dataset.collection_ids = ['collection_id_1', 'collection_id_2']

    def fetcher(collection_id):
        return [
            {'id': {'kind': 'youtube#video', 'videoId': f'{collection_id}_{i}'}}
            for i in range(3)
        ]

    with patch.object(dataset, 'fetch_videos', fetcher):
        assert list(dataset.items_list) == [
            {'id': {'kind': 'youtube#video', 'videoId': f'collection_id_1_0'}},
            {'id': {'kind': 'youtube#video', 'videoId': f'collection_id_1_1'}},
            {'id': {'kind': 'youtube#video', 'videoId': f'collection_id_1_2'}},
            {'id': {'kind': 'youtube#video', 'videoId': f'collection_id_2_0'}},
            {'id': {'kind': 'youtube#video', 'videoId': f'collection_id_2_1'}},
            {'id': {'kind': 'youtube#video', 'videoId': f'collection_id_2_2'}},
        ]


def test_get_item_key():
    dataset = YouTubeDataset(name="bla")
    video = {'id': {'kind': 'youtube#video', 'videoId': 'your_video_id'}, 'kind': 'youtube#searchResult'}
    assert dataset.get_item_key(video) == 'https://www.youtube.com/watch?v=your_video_id'


@pytest.mark.parametrize('error', (
    NoTranscriptFound('video_id', 'language_codes', 'transcript_data'),
    VideoUnavailable('video_id'),
    TranscriptsDisabled('video_id'),
))
def test_get_contents_with_no_transcript_found(error):
    dataset = YouTubeDataset(name="bla")
    video = {'id': {'kind': 'youtube#video', 'videoId': "bla_bla"}, 'kind': 'youtube#searchResult'}

    transcriber = Mock()
    transcriber.list_transcripts.return_value.find_transcript.return_value.fetch.side_effect = error

    with patch('align_data.sources.youtube.youtube.YouTubeTranscriptApi', transcriber):
        assert dataset._get_contents(video) is None


def test_get_contents():
    dataset = YouTubeDataset(name="bla")
    video = {'id': {'kind': 'youtube#video', 'videoId': "bla_bla"}, 'kind': 'youtube#searchResult'}

    transcriber = Mock()
    transcriber.list_transcripts.return_value.find_transcript.return_value.fetch.return_value = [
        {'text': 'bla bla'},
        {'text': 'second line'},
        {'text': 'ble ble'},
    ]

    with patch('align_data.sources.youtube.youtube.YouTubeTranscriptApi', transcriber):
        assert dataset._get_contents(video) == 'bla bla\nsecond line\nble ble'


def test_extract_authors_with_authors_defined():
    dataset = YouTubeDataset(name="bla")
    video = {'snippet': {'channelTitle': 'channel_title'}}

    dataset.authors = ['author_1', 'author_2']
    assert dataset.extract_authors(video) == ['author_1', 'author_2']


def test_extract_authors_with_no_authors_defined():
    dataset = YouTubeDataset(name="bla")
    video = {'snippet': {'channelTitle': 'channel title'}}

    dataset.authors = []
    assert dataset.extract_authors(video) == ['channel title']


def test_process_entry_with_valid_entry(transcriber):
    dataset = YouTubeDataset(name="bla")
    video = {
        'kind': 'youtube#searchResult',
        'id': {'kind': 'youtube#video', 'videoId': "bla_bla"},
        'snippet': {
            'title': 'bla bla title',
            'channelTitle': 'This is a pen!'
        }
    }

    assert dataset.process_entry(video).to_dict() == {
        "text": "bla bla\nsecond line\nble ble",
        "url": "https://www.youtube.com/watch?v=bla_bla",
        "title": "bla bla title",
        "source": "bla",
        "source_type": "youtube",
        "date_published": None,
        "authors": ["This is a pen!"],
        "summaries": [],
        "id": None,
    }


def test_channel_collection_ids():
    dataset = YouTubeChannelDataset(name='bla', channel_id='a channel id')
    assert dataset.collection_ids == ['a channel id']


def test_channel_published_date():
    dataset = YouTubeChannelDataset(name='bla', channel_id='a channel id')
    video = {
        'kind': 'youtube#searchResult',
        'id': {'kind': 'youtube#video', 'videoId': "bla_bla"},
        'snippet': {
            'title': 'bla bla title',
            'channelTitle': 'This is a pen!',
            'publishTime': '2022-01-02T03:04:05Z',
        }
    }
    assert dataset._get_published_date(video).isoformat() == '2022-01-02T03:04:05+00:00'


def test_channel_process_item(transcriber):
    dataset = YouTubeChannelDataset(name='bla', channel_id='a channel id')
    video = {
        'kind': 'youtube#searchResult',
        'id': {'kind': 'youtube#video', 'videoId': "bla_bla"},
        'snippet': {
            'title': 'bla bla title',
            'channelTitle': 'This is a pen!',
            'publishTime': '2022-01-02T03:04:05Z',
        }
    }
    assert dataset.process_entry(video).to_dict() == {
        'authors': ['This is a pen!'],
        'date_published': '2022-01-02T03:04:05Z',
        'id': None,
        'source': 'bla',
        'source_type': 'youtube',
        'summaries': [],
        'text': 'bla bla\nsecond line\nble ble',
        'title': 'bla bla title',
        'url': 'https://www.youtube.com/watch?v=bla_bla'
    }


def test_playlist_collection_ids():
    dataset = YouTubePlaylistDataset(name='bla', playlist_ids=['a list id', 'another id'])
    assert dataset.collection_ids == ['a list id', 'another id']


def test_playlist_published_date():
    dataset = YouTubePlaylistDataset(name='bla', playlist_ids=['a list id', 'another id'])
    video = {
        'kind': 'youtube#playlistItem',
        'snippet': {
            'resourceId': {'kind': 'youtube#video', 'videoId': "bla_bla"},
            'title': 'bla bla title',
            'channelTitle': 'This is a pen!',
            'publishedAt': '2022-01-02T03:04:05Z',
        }
    }
    assert dataset._get_published_date(video).isoformat() == '2022-01-02T03:04:05+00:00'


def test_channel_process_item(transcriber):
    dataset = YouTubePlaylistDataset(name='bla', playlist_ids=['a list id', 'another id'])
    video = {
        'kind': 'youtube#playlistItem',
        'snippet': {
            'resourceId': {'kind': 'youtube#video', 'videoId': "bla_bla"},
            'title': 'bla bla title',
            'channelTitle': 'This is a pen!',
            'publishedAt': '2022-01-02T03:04:05Z',
        }
    }
    assert dataset.process_entry(video).to_dict() == {
        'authors': ['This is a pen!'],
        'date_published': '2022-01-02T03:04:05Z',
        'id': None,
        'source': 'bla',
        'source_type': 'youtube',
        'summaries': [],
        'text': 'bla bla\nsecond line\nble ble',
        'title': 'bla bla title',
        'url': 'https://www.youtube.com/watch?v=bla_bla'
    }
