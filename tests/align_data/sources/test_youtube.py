from datetime import datetime
from unittest.mock import patch, Mock
import pytest
import requests
from align_data.sources.youtube import youtube as yt
from align_data.sources.youtube.youtube import (
    YouTubeDataset,
    YouTubeChannelDataset,
    YouTubePlaylistDataset,
    VideoUnavailable,
    FetchFailed,
)

TRANSCRIPT = "[00:00] bla bla second line ble ble"


@pytest.fixture
def transcriber():
    with patch(
        "align_data.sources.youtube.youtube.fetch_transcript", return_value=TRANSCRIPT
    ) as fetch:
        yield fetch


def test_key_required(monkeypatch):
    monkeypatch.setattr("align_data.sources.youtube.youtube.YOUTUBE_API_KEY", None)
    dataset = YouTubeDataset(name="asd")
    with pytest.raises(ValueError, match="No YOUTUBE_API_KEY provided"):
        dataset.setup()


def test_next_page_empty_by_default():
    dataset = YouTubeDataset(name="asd")
    assert not dataset.next_page("collection id", "token")["items"]


@pytest.mark.parametrize(
    "item",
    (
        {
            "kind": "youtube#searchResult",
            "id": {"kind": "youtube#video", "videoId": "your_video_id"},
        },
        {
            "kind": "youtube#playlistItem",
            "snippet": {"resourceId": {"kind": "youtube#video", "videoId": "your_video_id"}},
        },
    ),
)
def test_get_id_with_id(item):
    dataset = YouTubeDataset(name="bla")
    result = dataset._get_id(item)
    assert result == "your_video_id"


@pytest.mark.parametrize(
    "item",
    (
        {"bla": "bla"},
        {
            "kind": "invalid_kind",
            "id": {"kind": "youtube#video", "videoId": "your_video_id"},
        },
        {
            "kind": "youtube#searchResult",
            "id": {"kind": "bla bla bla", "videoId": "your_video_id"},
        },
        {
            "kind": "youtube#playlistItem",
            "snippet": {"resourceId": {"kind": "invalid_kind", "videoId": "your_video_id"}},
        },
    ),
)
def test_get_id_with_invalid_id(item):
    dataset = YouTubeDataset(name="bla")
    result = dataset._get_id(item)
    assert result is None


def test_fetch_videos_default():
    dataset = YouTubeDataset(name="bla")
    assert list(dataset.fetch_videos("collection")) == []


def test_fetch_videos_with_next_page_token():
    dataset = YouTubeDataset(name="bla")

    items = [
        {
            "items": [
                {
                    "kind": "youtube#searchResult",
                    "id": {"kind": "youtube#video", "videoId": str(i)},
                }
                for i in range(0, 3)
            ],
            "nextPageToken": "1",
        },
        {
            "items": [
                {
                    "kind": "youtube#searchResult",
                    "id": {"kind": "youtube#video", "videoId": str(i)},
                }
                for i in range(3, 6)
            ],
            "nextPageToken": "2",
        },
        {
            "items": [
                {
                    "kind": "youtube#searchResult",
                    "id": {"kind": "youtube#video", "videoId": str(i)},
                }
                for i in range(6, 9)
            ],
            "nextPageToken": None,
        },
    ]

    with patch.object(dataset, "next_page", side_effect=items):
        assert list(dataset.fetch_videos("collection_id")) == [
            {
                "id": {"kind": "youtube#video", "videoId": str(i)},
                "kind": "youtube#searchResult",
            }
            for i in range(9)
        ]


def test_fetch_videos_stops_when_no_next_page_token():
    dataset = YouTubeDataset(name="bla")

    items = [
        {
            "items": [
                {
                    "kind": "youtube#searchResult",
                    "id": {"kind": "youtube#video", "videoId": str(i)},
                }
                for i in range(0, 3)
            ],
            "nextPageToken": None,
        },
        {
            "items": [
                {
                    "kind": "youtube#searchResult",
                    "id": {"kind": "youtube#video", "videoId": str(i)},
                }
                for i in range(3, 6)
            ],
            "nextPageToken": "2",
        },
        {
            "items": [
                {
                    "kind": "youtube#searchResult",
                    "id": {"kind": "youtube#video", "videoId": str(i)},
                }
                for i in range(6, 9)
            ],
            "nextPageToken": "3",
        },
    ]

    with patch.object(dataset, "next_page", side_effect=items):
        assert list(dataset.fetch_videos("collection_id")) == [
            {
                "id": {"kind": "youtube#video", "videoId": str(i)},
                "kind": "youtube#searchResult",
            }
            for i in range(3)
        ]


def test_items_list():
    dataset = YouTubeDataset(name="bla")
    dataset.collection_ids = ["collection_id_1", "collection_id_2"]

    def fetcher(collection_id):
        return [
            {"id": {"kind": "youtube#video", "videoId": f"{collection_id}_{i}"}} for i in range(3)
        ]

    with patch.object(dataset, "fetch_videos", fetcher):
        assert list(dataset.items_list) == [
            {"id": {"kind": "youtube#video", "videoId": f"collection_id_1_0"}},
            {"id": {"kind": "youtube#video", "videoId": f"collection_id_1_1"}},
            {"id": {"kind": "youtube#video", "videoId": f"collection_id_1_2"}},
            {"id": {"kind": "youtube#video", "videoId": f"collection_id_2_0"}},
            {"id": {"kind": "youtube#video", "videoId": f"collection_id_2_1"}},
            {"id": {"kind": "youtube#video", "videoId": f"collection_id_2_2"}},
        ]


def test_get_item_key():
    dataset = YouTubeDataset(name="bla")
    video = {
        "id": {"kind": "youtube#video", "videoId": "your_video_id"},
        "kind": "youtube#searchResult",
    }
    assert dataset.get_item_key(video) == "https://www.youtube.com/watch?v=your_video_id"


@pytest.mark.parametrize(
    "fetched, failed",
    (
        (VideoUnavailable("Video is UNPLAYABLE"), 0),
        (FetchFailed("bot check"), 1),
        (None, 0),
    ),
)
def test_get_contents_without_transcript(fetched, failed):
    dataset = YouTubeDataset(name="bla")
    video = {
        "id": {"kind": "youtube#video", "videoId": "bla_bla"},
        "kind": "youtube#searchResult",
    }
    kwargs = {"side_effect": fetched} if isinstance(fetched, Exception) else {"return_value": fetched}
    with patch("align_data.sources.youtube.youtube.fetch_transcript", **kwargs):
        assert dataset._get_contents(video) is None
    assert dataset.stats == {"attempted": 1, "failed": failed}


def test_get_contents(transcriber):
    dataset = YouTubeDataset(name="bla")
    video = {
        "id": {"kind": "youtube#video", "videoId": "bla_bla"},
        "kind": "youtube#searchResult",
    }
    assert dataset._get_contents(video) == TRANSCRIPT
    transcriber.assert_called_once_with("bla_bla", yt.YT_PROXY_URL)


def test_extract_authors_with_authors_defined():
    dataset = YouTubeDataset(name="bla")
    video = {"snippet": {"channelTitle": "channel_title"}}

    dataset.authors = ["author_1", "author_2"]
    assert dataset.extract_authors(video) == ["author_1", "author_2"]


def test_extract_authors_with_no_authors_defined():
    dataset = YouTubeDataset(name="bla")
    video = {"snippet": {"channelTitle": "channel title"}}

    dataset.authors = []
    assert dataset.extract_authors(video) == ["channel title"]


def test_process_entry_with_valid_entry(transcriber):
    dataset = YouTubeDataset(name="bla")
    video = {
        "kind": "youtube#searchResult",
        "id": {"kind": "youtube#video", "videoId": "bla_bla"},
        "snippet": {"title": "bla bla title", "channelTitle": "This is a pen!"},
    }

    assert dataset.process_entry(video).to_dict() == {
        "text": TRANSCRIPT,
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
    dataset = YouTubeChannelDataset(name="bla", channel_id="a channel id")
    assert dataset.collection_ids == ["a channel id"]


def test_channel_published_date():
    dataset = YouTubeChannelDataset(name="bla", channel_id="a channel id")
    video = {
        "kind": "youtube#searchResult",
        "id": {"kind": "youtube#video", "videoId": "bla_bla"},
        "snippet": {
            "title": "bla bla title",
            "channelTitle": "This is a pen!",
            "publishTime": "2022-01-02T03:04:05Z",
        },
    }
    assert dataset._get_published_date(video).isoformat() == "2022-01-02T03:04:05+00:00"


def test_channel_process_item(transcriber):
    dataset = YouTubeChannelDataset(name="bla", channel_id="a channel id")
    video = {
        "kind": "youtube#searchResult",
        "id": {"kind": "youtube#video", "videoId": "bla_bla"},
        "snippet": {
            "title": "bla bla title",
            "channelTitle": "This is a pen!",
            "publishTime": "2022-01-02T03:04:05Z",
        },
    }
    assert dataset.process_entry(video).to_dict() == {
        "authors": ["This is a pen!"],
        "date_published": "2022-01-02T03:04:05Z",
        "id": None,
        "source": "bla",
        "source_type": "youtube",
        "summaries": [],
        "text": TRANSCRIPT,
        "title": "bla bla title",
        "url": "https://www.youtube.com/watch?v=bla_bla",
    }


def test_playlist_collection_ids():
    dataset = YouTubePlaylistDataset(name="bla", playlist_ids=["a list id", "another id"])
    assert dataset.collection_ids == ["a list id", "another id"]


def test_playlist_authors_use_video_uploader():
    dataset = YouTubePlaylistDataset(
        name="test_playlist",
        playlist_ids=["test_playlist_id"],
    )
    video = {
        "kind": "youtube#playlistItem",
        "snippet": {
            "channelTitle": "Playlist curator",
            "videoOwnerChannelTitle": "  Video uploader  ",
        },
    }

    assert dataset.extract_authors(video) == ["Video uploader"]


@pytest.mark.parametrize("uploader", [None, "", "   "])
def test_playlist_authors_fall_back_when_uploader_empty(uploader):
    dataset = YouTubePlaylistDataset(name="test_playlist", playlist_ids=["playlist"])
    video = {
        "kind": "youtube#playlistItem",
        "snippet": {
            "channelTitle": "  Playlist curator  ",
            "videoOwnerChannelTitle": uploader,
        },
    }

    assert dataset.extract_authors(video) == ["Playlist curator"]


def test_playlist_authors_preserve_explicit_override():
    dataset = YouTubePlaylistDataset(name="test_playlist", playlist_ids=["playlist"])
    dataset.authors = ["Configured author"]
    video = {
        "kind": "youtube#playlistItem",
        "snippet": {
            "channelTitle": "Playlist curator",
            "videoOwnerChannelTitle": "Video uploader",
        },
    }

    assert dataset.extract_authors(video) == ["Configured author"]


def test_channel_authors_still_use_channel_title():
    dataset = YouTubeChannelDataset(name="test_channel", channel_id="channel", authors=[])
    video = {
        "kind": "youtube#searchResult",
        "snippet": {
            "channelTitle": "  Channel author  ",
            "videoOwnerChannelTitle": "Other author",
        },
    }

    assert dataset.extract_authors(video) == ["Channel author"]


def test_playlist_published_date():
    dataset = YouTubePlaylistDataset(name="bla", playlist_ids=["a list id", "another id"])
    video = {
        "kind": "youtube#playlistItem",
        "snippet": {
            "resourceId": {"kind": "youtube#video", "videoId": "bla_bla"},
            "title": "bla bla title",
            "channelTitle": "This is a pen!",
            "publishedAt": "2022-01-02T03:04:05Z",
        },
    }
    assert dataset._get_published_date(video).isoformat() == "2022-01-02T03:04:05+00:00"


def test_playlist_process_item(transcriber):
    dataset = YouTubePlaylistDataset(name="bla", playlist_ids=["a list id", "another id"])
    video = {
        "kind": "youtube#playlistItem",
        "snippet": {
            "resourceId": {"kind": "youtube#video", "videoId": "bla_bla"},
            "title": "bla bla title",
            "channelTitle": "This is a pen!",
            "publishedAt": "2022-01-02T03:04:05Z",
        },
    }
    assert dataset.process_entry(video).to_dict() == {
        "authors": ["This is a pen!"],
        "date_published": "2022-01-02T03:04:05Z",
        "id": None,
        "source": "bla",
        "source_type": "youtube",
        "summaries": [],
        "text": TRANSCRIPT,
        "title": "bla bla title",
        "url": "https://www.youtube.com/watch?v=bla_bla",
    }


def test_playlist_item_prefers_video_date_and_owner():
    dataset = YouTubePlaylistDataset(name="bla", playlist_ids=["a list id"])
    video = {
        "kind": "youtube#playlistItem",
        "snippet": {
            "resourceId": {"kind": "youtube#video", "videoId": "bla_bla"},
            "title": "bla bla title",
            "channelTitle": "Playlist owner",
            "videoOwnerChannelTitle": "Video owner",
            "publishedAt": "2024-05-05T00:00:00Z",
        },
        "contentDetails": {"videoPublishedAt": "2022-01-02T03:04:05Z"},
    }
    assert dataset._get_published_date(video).isoformat() == "2022-01-02T03:04:05+00:00"
    assert dataset.extract_authors(video) == ["Video owner"]


def test_uploads_playlist_id():
    assert yt.uploads_playlist_id("UCLB7AzTwc6VFZrBsO2ucBMg") == "UULB7AzTwc6VFZrBsO2ucBMg"
    with pytest.raises(ValueError):
        yt.uploads_playlist_id("PLsomething")


def test_channel_lists_uploads_playlist():
    dataset = YouTubeChannelDataset(name="bla", channel_id="UCabc", authors=[])
    dataset.youtube = Mock()
    dataset.youtube.playlistItems.return_value.list.return_value.execute.return_value = {"items": []}
    dataset.next_page("UCabc", "token")
    dataset.youtube.playlistItems.return_value.list.assert_called_once_with(
        part="snippet,contentDetails", playlistId="UUabc", maxResults=50, pageToken="token"
    )
    dataset.youtube.search.assert_not_called()


@pytest.mark.parametrize(
    "seconds, expected",
    ((0, "[00:00]"), (59.9, "[00:59]"), (61, "[01:01]"), (3600, "[1:00:00]"), (3725, "[1:02:05]")),
)
def test_format_timestamp(seconds, expected):
    assert yt.format_timestamp(seconds) == expected


def test_captions_to_text():
    events = [
        {"tStartMs": 0, "segs": [{"utf8": "hello"}, {"utf8": " there"}]},
        {"tStartMs": 1000, "aAppend": 1, "segs": [{"utf8": "\n"}]},
        {"tStartMs": 2000},
        {"tStartMs": 12000, "segs": [{"utf8": "general  kenobi"}]},
        {"tStartMs": 31000, "segs": [{"utf8": "second paragraph"}]},
        {"tStartMs": 45000, "segs": [{"utf8": "still second"}]},
        {"tStartMs": 3700000, "segs": [{"utf8": "an hour in"}]},
    ]
    assert yt.captions_to_text(events) == (
        "[00:00] hello there general kenobi\n\n"
        "[00:31] second paragraph still second\n\n"
        "[1:01:40] an hour in"
    )


def test_captions_to_text_empty():
    assert yt.captions_to_text([{"tStartMs": 0, "segs": [{"utf8": "\n"}]}]) == ""


@pytest.mark.parametrize(
    "tracks, expected",
    (
        ([], None),
        ([{"languageCode": "fr", "kind": ""}], None),
        ([{"languageCode": "en", "kind": "asr"}, {"languageCode": "en-GB"}], {"languageCode": "en-GB"}),
        ([{"languageCode": "en-US"}, {"languageCode": "en"}], {"languageCode": "en"}),
        (
            [{"languageCode": "fr"}, {"languageCode": "en", "kind": "asr"}],
            {"languageCode": "en", "kind": "asr"},
        ),
    ),
)
def test_pick_caption_track(tracks, expected):
    assert yt.pick_caption_track(tracks) == expected


def test_to_json3_url():
    url = yt.to_json3_url("https://www.youtube.com/api/timedtext?v=abc&fmt=srv3&sig=x%2By")
    assert "fmt=json3" in url and "fmt=srv3" not in url
    assert "sig=x%2By" in url and "v=abc" in url


def _response(json_data=None, status=200, content=b"x"):
    resp = Mock()
    resp.json.return_value = json_data
    resp.content = content
    resp.status_code = status
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(f"{status}")
    return resp


PLAYER_OK = {
    "playabilityStatus": {"status": "OK"},
    "captions": {
        "playerCaptionsTracklistRenderer": {
            "captionTracks": [
                {"baseUrl": "https://www.youtube.com/api/timedtext?v=v1&fmt=srv3", "languageCode": "en", "kind": "asr"},
            ]
        }
    },
}
LOGIN_REQUIRED = {"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you're not a bot"}}


def test_fetch_transcript_mints_via_proxy_and_fetches_captions_directly():
    def post(*args, **kwargs):
        return _response(PLAYER_OK)

    events = {"events": [{"tStartMs": 0, "segs": [{"utf8": "hi"}]}]}
    with patch("align_data.sources.youtube.youtube.requests.Session") as Session, patch(
        "align_data.sources.youtube.youtube.requests.get", return_value=_response(events)
    ) as get:
        session = Session.return_value.__enter__.return_value
        session.post.side_effect = post
        assert yt.fetch_transcript("v1", "http://u:p@proxy:1") == "[00:00] hi"

    assert session.proxies == {"http": "http://u:p@proxy:1", "https": "http://u:p@proxy:1"}
    _, kwargs = session.post.call_args
    assert kwargs["json"]["videoId"] == "v1"
    assert kwargs["params"]["fields"] == yt.PLAYER_FIELDS
    # the captions themselves are fetched without the proxy, as json3
    (url,), get_kwargs = get.call_args
    assert "fmt=json3" in url and "proxies" not in get_kwargs


def test_mint_retries_with_a_fresh_session_per_attempt():
    with patch("align_data.sources.youtube.youtube.requests.Session") as Session:
        session = Session.return_value.__enter__.return_value
        session.post.side_effect = [_response(LOGIN_REQUIRED), _response(status=502), _response(PLAYER_OK)]
        assert yt.mint_player_response("v1", "http://proxy:1") == PLAYER_OK
    assert Session.call_count == 3


def test_mint_gives_up_after_attempts():
    with patch("align_data.sources.youtube.youtube.requests.Session") as Session:
        session = Session.return_value.__enter__.return_value
        session.post.return_value = _response(LOGIN_REQUIRED)
        with pytest.raises(FetchFailed, match="bot check"):
            yt.mint_player_response("v1", "http://proxy:1")
    assert Session.call_count == yt.MINT_ATTEMPTS_VIA_PROXY


def test_mint_without_proxy_tries_once():
    with patch("align_data.sources.youtube.youtube.requests.Session") as Session:
        session = Session.return_value.__enter__.return_value
        session.post.return_value = _response(LOGIN_REQUIRED)
        with pytest.raises(FetchFailed, match="YT_PROXY_URL is not set"):
            yt.mint_player_response("v1", None)
    assert Session.call_count == 1


def test_mint_unplayable_video_is_not_retried():
    with patch("align_data.sources.youtube.youtube.requests.Session") as Session:
        session = Session.return_value.__enter__.return_value
        session.post.return_value = _response({"playabilityStatus": {"status": "ERROR", "reason": "Video unavailable"}})
        with pytest.raises(VideoUnavailable, match="Video unavailable"):
            yt.mint_player_response("v1", "http://proxy:1")
    assert Session.call_count == 1


def test_fetch_transcript_without_captions():
    with patch("align_data.sources.youtube.youtube.mint_player_response", return_value={"playabilityStatus": {"status": "OK"}}):
        assert yt.fetch_transcript("v1", None) is None


def test_caption_fetch_retries_once_then_fails():
    with patch("align_data.sources.youtube.youtube.requests.get", return_value=_response(status=500)) as get:
        with pytest.raises(FetchFailed):
            yt.fetch_caption_events({"baseUrl": "https://www.youtube.com/api/timedtext?v=v1"})
    assert get.call_count == 2


@pytest.mark.parametrize(
    "attempted, failed, fails",
    ((0, 0, False), (10, 5, False), (100, 20, False), (100, 21, True), (6, 6, True)),
)
def test_finish_fails_the_run_when_many_fetches_failed(attempted, failed, fails):
    dataset = YouTubeDataset(name="bla")
    dataset.stats.update(attempted=attempted, failed=failed)
    if fails:
        with pytest.raises(RuntimeError, match=f"{failed} of {attempted}"):
            dataset.finish()
    else:
        dataset.finish()


def test_multidataset_finish_collects_failures():
    from align_data.common.alignment_dataset import MultiDataset

    ok, bad1, bad2 = (YouTubeDataset(name=n) for n in ("ok", "bad1", "bad2"))
    bad1.stats.update(attempted=10, failed=10)
    bad2.stats.update(attempted=8, failed=7)
    with pytest.raises(RuntimeError, match="bad1.*bad2"):
        MultiDataset(name="youtube", datasets=[ok, bad1, bad2]).finish()


@pytest.mark.parametrize(
    "reason, is_bot_check",
    (
        ("Sign in to confirm you're not a bot", True),
        (None, True),
        ("This video may be inappropriate for some users.", False),
        ("This video is private", False),
    ),
)
def test_login_required_is_a_bot_check_only_when_it_says_so(reason, is_bot_check):
    playability = {"status": "LOGIN_REQUIRED"}
    if reason:
        playability["reason"] = reason
    with patch("align_data.sources.youtube.youtube.requests.Session") as Session:
        session = Session.return_value.__enter__.return_value
        session.post.return_value = _response({"playabilityStatus": playability})
        if is_bot_check:
            with pytest.raises(yt.FetchFailed):
                yt.mint_player_response("v1", "http://proxy")
            assert session.post.call_count == yt.MINT_ATTEMPTS_VIA_PROXY
        else:
            with pytest.raises(yt.VideoUnavailable):
                yt.mint_player_response("v1", "http://proxy")
            assert session.post.call_count == 1


def test_youtube_multidataset_checks_failures_across_sources():
    # A dead proxy with daily runs: every channel has a few new videos, all failed,
    # none over the per-source threshold on its own.
    datasets = [YouTubeDataset(name=f"c{i}") for i in range(4)]
    for d in datasets:
        d.stats.update(attempted=3, failed=3)
    with pytest.raises(RuntimeError, match="youtube: 12 of 12"):
        yt.YouTubeMultiDataset(name="youtube", datasets=datasets).finish()


def test_youtube_multidataset_passes_when_few_failed():
    datasets = [YouTubeDataset(name=f"c{i}") for i in range(4)]
    for d in datasets:
        d.stats.update(attempted=30, failed=1)
    yt.YouTubeMultiDataset(name="youtube", datasets=datasets).finish()


def test_run_stops_after_many_failures_in_a_row():
    dataset = YouTubeDataset(name="bla")
    video = {"kind": "youtube#playlistItem", "snippet": {"resourceId": {"kind": "youtube#video", "videoId": "v1"}}}
    with patch.object(yt, "_consecutive_failures", 0), patch.object(
        yt, "fetch_transcript", side_effect=yt.FetchFailed("proxy down")
    ):
        for _ in range(yt.MAX_CONSECUTIVE_FAILURES - 1):
            assert dataset._get_contents(video) is None
        with pytest.raises(RuntimeError, match="in a row"):
            dataset._get_contents(video)


def test_a_success_resets_the_failures_in_a_row():
    dataset = YouTubeDataset(name="bla")
    video = {"kind": "youtube#playlistItem", "snippet": {"resourceId": {"kind": "youtube#video", "videoId": "v1"}}}
    outcomes = [yt.FetchFailed("x")] * (yt.MAX_CONSECUTIVE_FAILURES - 1) + ["text"] + [yt.FetchFailed("x")] * (
        yt.MAX_CONSECUTIVE_FAILURES - 1
    )
    with patch.object(yt, "_consecutive_failures", 0), patch.object(yt, "fetch_transcript", side_effect=outcomes):
        for _ in outcomes:
            dataset._get_contents(video)
