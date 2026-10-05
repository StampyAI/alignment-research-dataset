"""YouTube videos as articles: the text is the video's captions with timestamps.

YouTube blocks caption requests from cloud IPs (GitHub Actions included), so
the transcript is fetched in two steps, as in the Lens editor's importer:

 - Only the caption-URL *mint* (one ~4KB innertube /player POST, ANDROID
   client, `fields`-trimmed) needs a clean IP. It goes through YT_PROXY_URL
   (a rotating residential proxy) when that is set.
 - The minted timedtext URL is signed but not IP-locked, and `fmt` is not part
   of its signature, so the captions themselves are fetched directly (no
   proxy bandwidth) as json3.

The ANDROID client is used because its caption URLs work without the
proof-of-origin token that WEB-client URLs now require.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Iterable
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import requests
from googleapiclient.discovery import build, Resource

from align_data.settings import YOUTUBE_API_KEY, YT_PROXY_URL
from align_data.common.alignment_dataset import AlignmentDataset
from align_data.db.models import Article

logger = logging.getLogger(__name__)

PLAYER_URL = "https://www.youtube.com/youtubei/v1/player"
# Trims the ~130KB player response to the ~4KB we use -- this is the only
# payload that crosses the (metered) proxy.
PLAYER_FIELDS = (
    "playabilityStatus(status,reason),"
    "videoDetails(videoId,title,author),"
    "captions.playerCaptionsTracklistRenderer.captionTracks(baseUrl,languageCode,kind)"
)
ANDROID_CONTEXT = {
    "client": {
        "clientName": "ANDROID",
        "clientVersion": "20.10.38",
        "androidSdkVersion": 30,
        "hl": "en",
    }
}
ANDROID_UA = "com.google.android.youtube/20.10.38 (Linux; U; Android 11) gzip"
# YouTube flags a few percent of residential exit IPs as bots. A rotating proxy
# hands out a fresh exit IP per new connection, so each attempt uses its own
# requests.Session. Without a proxy a retry would reuse the same IP, so we only
# try once.
MINT_ATTEMPTS_VIA_PROXY = 5
MINT_TIMEOUT = 30
CAPTION_TIMEOUT = 60
# A timestamp marker starts a new paragraph at most this often.
MARKER_EVERY_SECONDS = 30
# A run fails when more than this share of caption fetches failed (and more
# than MAX_SILENT_FAILURES of them), so a blocked scraper shows up as a failed
# job instead of a quiet "success".
MAX_FAILURE_SHARE = 0.2
MAX_SILENT_FAILURES = 5


class VideoUnavailable(Exception):
    """The video itself can't be fetched (private, removed, age-gated...). Not retried."""


class FetchFailed(Exception):
    """The fetch failed for a reason that says nothing about the video: a bot
    check, a network error, a bad response. Counted towards failing the run."""


def mint_player_response(video_id: str, proxy_url: Optional[str]) -> dict:
    attempts = MINT_ATTEMPTS_VIA_PROXY if proxy_url else 1
    last_error = "YouTube player request failed"
    for attempt in range(1, attempts + 1):
        # A fresh session per attempt means a fresh connection, so a fresh exit IP.
        with requests.Session() as session:
            if proxy_url:
                session.proxies = {"http": proxy_url, "https": proxy_url}
            try:
                resp = session.post(
                    PLAYER_URL,
                    params={"prettyPrint": "false", "fields": PLAYER_FIELDS},
                    json={"videoId": video_id, "context": ANDROID_CONTEXT},
                    headers={"User-Agent": ANDROID_UA},
                    timeout=MINT_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
            except (requests.RequestException, ValueError) as e:
                last_error = f"{type(e).__name__}: {e}"
                logger.warning(f"Mint attempt {attempt}/{attempts} for {video_id} failed: {last_error}")
                continue

        playability = data.get("playabilityStatus") or {}
        status = playability.get("status")
        if status == "LOGIN_REQUIRED":
            last_error = "YouTube bot check rejected the request (exit IP flagged" + (
                ")" if proxy_url else "; YT_PROXY_URL is not set)"
            )
            logger.warning(f"Mint attempt {attempt}/{attempts} for {video_id} failed: {last_error}")
            continue
        if status and status != "OK":
            reason = playability.get("reason")
            raise VideoUnavailable(f"Video is {status}" + (f" ({reason})" if reason else ""))
        return data
    raise FetchFailed(last_error)


def pick_caption_track(tracks: List[dict]) -> Optional[dict]:
    """The best English track: human-written before auto-generated ("asr"),
    then `en` before regional variants. None when there is no English track."""
    english = [t for t in tracks if (t.get("languageCode") or "").startswith("en")]
    if not english:
        return None

    def rank(track):
        auto = 2 if track.get("kind") == "asr" else 0
        lang = 0 if track.get("languageCode") == "en" else 1
        return auto + lang

    # min keeps the earliest track on ties
    return min(english, key=rank)


def to_json3_url(base_url: str) -> str:
    parts = urlparse(base_url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query["fmt"] = ["json3"]
    return urlunparse(parts._replace(query=urlencode(query, doseq=True)))


def fetch_caption_events(track: dict) -> List[dict]:
    """The track's json3 caption events, fetched directly (not via the proxy)."""
    url = to_json3_url(track["baseUrl"])
    error = "Caption response was empty"
    # One retry: a transient 5xx or empty body shouldn't lose the video.
    for _ in range(2):
        try:
            resp = requests.get(url, headers={"User-Agent": ANDROID_UA}, timeout=CAPTION_TIMEOUT)
            resp.raise_for_status()
            if resp.content:
                return resp.json().get("events") or []
        except (requests.RequestException, ValueError) as e:
            error = f"{type(e).__name__}: {e}"
    raise FetchFailed(f"Caption fetch failed: {error}")


def format_timestamp(seconds: float) -> str:
    seconds = int(seconds)
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    return f"[{hours}:{minutes:02}:{secs:02}]" if hours else f"[{minutes:02}:{secs:02}]"


def captions_to_text(events: List[dict], every: int = MARKER_EVERY_SECONDS) -> str:
    """Join caption events into paragraphs, each starting with a timestamp,
    a new paragraph at most every `every` seconds."""
    paragraphs, current, start = [], [], None
    for event in events:
        text = "".join(seg.get("utf8", "") for seg in event.get("segs") or [])
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        at = event.get("tStartMs", 0) / 1000
        if start is None or at - start >= every:
            if current:
                paragraphs.append(f"{format_timestamp(start)} {' '.join(current)}")
            current, start = [], at
        current.append(text)
    if current:
        paragraphs.append(f"{format_timestamp(start)} {' '.join(current)}")
    return "\n\n".join(paragraphs)


def fetch_transcript(video_id: str, proxy_url: Optional[str]) -> Optional[str]:
    """The video's English captions as timestamped text, or None when it has none.

    Raises VideoUnavailable for videos that can't be fetched, and FetchFailed
    for everything else that went wrong.
    """
    player = mint_player_response(video_id, proxy_url)
    tracks = ((player.get("captions") or {}).get("playerCaptionsTracklistRenderer") or {}).get(
        "captionTracks"
    ) or []
    track = pick_caption_track(tracks)
    if not track:
        return None
    return captions_to_text(fetch_caption_events(track)) or None


class YouTubeDataset(AlignmentDataset):
    done_key = "url"
    batch_size = 1
    # COOLDOWN = 2
    authors: Optional[List[str]] = None

    def setup(self):
        super().setup()
        if not YOUTUBE_API_KEY:
            raise ValueError("No YOUTUBE_API_KEY provided!")
        self.youtube: Resource = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    def next_page(self, collection_id: str, next_page_token: list) -> dict:
        return {"items": []}

    @staticmethod
    def _get_id(item) -> str | None:
        if item.get("kind") == "youtube#searchResult":
            resource = item["id"]
        elif item.get("kind") == "youtube#playlistItem":
            resource = item["snippet"]["resourceId"]
        else:
            return None

        if resource["kind"] == "youtube#video":
            return resource["videoId"]

    def fetch_videos(self, collection_id: str) -> Iterable[dict]:
        next_page_token = None
        while True:
            videos_response = self.next_page(collection_id, next_page_token)

            for item in videos_response.get("items"):
                if self._get_id(item):
                    yield item

            next_page_token = videos_response.get("nextPageToken")
            if not next_page_token:
                return

    def list_playlist(self, playlist_id: str, next_page_token) -> dict:
        """One page of a playlist (1 quota unit per page)."""
        return (
            self.youtube.playlistItems()
            .list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            .execute()
        )

    @property
    def items_list(self):
        return (
            video
            for collection_id in self.collection_ids
            for video in self.fetch_videos(collection_id)
        )

    @property
    def collection_ids(self) -> List[str]:
        return getattr(self, "_collection_ids", [])

    @collection_ids.setter
    def collection_ids(self, value: List[str]):
        self._collection_ids = value

    def get_item_key(self, item) -> str:
        video_id = self._get_id(item)
        if video_id is None:
            raise ValueError("Invalid video item; missing videoId")
        return f"https://www.youtube.com/watch?v={video_id}"

    @property
    def stats(self) -> dict:
        if not hasattr(self, "_stats"):
            self._stats = {"attempted": 0, "failed": 0}
        return self._stats

    def _get_contents(self, video):
        video_id = self._get_id(video)
        url = f"https://www.youtube.com/watch?v={video_id}"
        logger.debug(f"Fetching transcript for video: {video_id} - {video.get('snippet', {}).get('title', 'Unknown title')}")
        self.stats["attempted"] += 1
        try:
            text = fetch_transcript(video_id, YT_PROXY_URL)
        except VideoUnavailable as e:
            logger.info(f"Skipping {url}: {e}")
            return None
        except FetchFailed as e:
            self.stats["failed"] += 1
            logger.error(f"Could not fetch the transcript for {url}: {e}")
            return None
        if not text:
            logger.info(f"Skipping {url}: no English captions")
        return text

    def finish(self):
        attempted, failed = self.stats["attempted"], self.stats["failed"]
        if not attempted:
            return
        logger.info(f"{self.name}: {failed} of {attempted} transcript fetches failed")
        if failed > MAX_SILENT_FAILURES and failed / attempted > MAX_FAILURE_SHARE:
            raise RuntimeError(
                f"{self.name}: {failed} of {attempted} transcript fetches failed "
                f"(YouTube blocking or network trouble; is YT_PROXY_URL set and working?)"
            )

    def extract_authors(self, video):
        if self.authors:
            return self.authors
        snippet = video["snippet"]
        # In a playlist, channelTitle is the playlist's owner; videoOwnerChannelTitle is the video's.
        return [(snippet.get("videoOwnerChannelTitle") or snippet["channelTitle"]).strip()]

    @staticmethod
    def _extract_published_date(video) -> str | None:
        if not isinstance(video, dict):
            return None
        # contentDetails.videoPublishedAt is when the video went up; a playlist
        # item's snippet.publishedAt is when it was added to the playlist.
        details = video.get("contentDetails") or {}
        snippet = video.get("snippet") or {}
        return details.get("videoPublishedAt") or snippet.get("publishTime") or snippet.get("publishedAt")

    @staticmethod
    def _get_published_date(date) -> datetime | None:  # type: ignore[override]
        if isinstance(date, dict):
            date = YouTubeDataset._extract_published_date(date)
        return AlignmentDataset._get_published_date(date)

    def process_entry(self, video) -> Article | None:
        video_url = self.get_item_key(video)
        contents = self._get_contents(video)

        if not contents:
            return None

        return self.make_data_entry(
            {
                "text": contents,
                "url": video_url,
                "title": video["snippet"]["title"],
                "source": self.name,
                "source_type": "youtube",
                "date_published": self._get_published_date(video),
                "authors": self.extract_authors(video),
            }
        )


def uploads_playlist_id(channel_id: str) -> str:
    """Every channel's uploads are a playlist whose id is the channel id with UC -> UU."""
    if not channel_id.startswith("UC"):
        raise ValueError(f"Not a channel id: {channel_id}")
    return "UU" + channel_id[2:]


@dataclass
class YouTubeChannelDataset(YouTubeDataset):
    channel_id: str
    authors: List[str]

    @property
    def collection_ids(self):
        return [self.channel_id]

    def next_page(self, collection_id, next_page_token):
        # The uploads playlist lists every video at 1 quota unit per page;
        # search().list costs 100 per page and stops at about 500 results.
        return self.list_playlist(uploads_playlist_id(collection_id), next_page_token)


@dataclass
class YouTubePlaylistDataset(YouTubeDataset):

    playlist_ids: List[str]

    @property
    def collection_ids(self):
        return self.playlist_ids

    def next_page(self, collection_id: str, next_page_token: list):
        return self.list_playlist(collection_id, next_page_token)
