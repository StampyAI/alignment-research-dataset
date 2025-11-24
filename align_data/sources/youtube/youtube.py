import logging
from datetime import datetime
from dataclasses import dataclass, field
import os
from typing import List, Optional, Iterable
from xml.etree.ElementTree import ParseError

from googleapiclient.discovery import build, Resource
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.proxies import WebshareProxyConfig
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    VideoUnavailable,
    TranscriptsDisabled,
)

from align_data.settings import YOUTUBE_API_KEY
from align_data.common.alignment_dataset import AlignmentDataset
from align_data.db.models import Article

logger = logging.getLogger(__name__)


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

    def _get_transcript_api(self):
        """
        Lazily construct the transcript client so callers (and tests) don't
        need to remember to call setup() before fetching transcripts.
        """
        if not hasattr(self, "_transcript_api"):
            self._transcript_api = YouTubeTranscriptApi()
        return self._transcript_api

    def _get_contents(self, video):
        video_id = self._get_id(video)
        logger.debug(f"Fetching transcript for video: {video_id} - {video.get('snippet', {}).get('title', 'Unknown title')}")
        try:
            transcript = (
                self._get_transcript_api()
                .list(video_id)
                .find_transcript(["en", "en-GB"])
                .fetch()
                .to_raw_data()
            )
            return "\n".join([i["text"] for i in transcript])
        except (NoTranscriptFound, VideoUnavailable):
            return None
        except TranscriptsDisabled:
            logger.error(
                f"Transcripts disabled for https://www.youtube.com/watch?v={video_id} - skipping"
            )
            return None
        except ParseError as e:
            logger.warning(
                f"Empty or malformed transcript XML for https://www.youtube.com/watch?v={video_id} "
                f"(ParseError: {e}) - likely deleted/private video or corrupted transcript data - skipping"
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error fetching transcript for https://www.youtube.com/watch?v={video_id}: "
                f"{type(e).__name__}: {e} - skipping"
            )
            return None

    def extract_authors(self, video):
        if self.authors:
            return self.authors
        return [video["snippet"]["channelTitle"].strip()]

    @staticmethod
    def _extract_published_date(video) -> str | None:
        snippet = video.get("snippet", {}) if isinstance(video, dict) else {}
        return snippet.get("publishTime") or snippet.get("publishedAt")

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
                "date_published": self._get_published_date(
                    self._extract_published_date(video)
                ),
                "authors": self.extract_authors(video),
            }
        )


@dataclass
class YouTubeChannelDataset(YouTubeDataset):
    channel_id: str
    authors: List[str]

    @property
    def collection_ids(self):
        return [self.channel_id]

    def next_page(self, collection_id, next_page_token):
        return (
            self.youtube.search()
            .list(
                part="snippet",
                channelId=collection_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            .execute()
        )

    @staticmethod
    def _extract_published_date(video) -> str | None:
        return video.get("snippet", {}).get("publishTime")

    @staticmethod
    def _get_published_date(date) -> datetime | None:  # type: ignore[override]
        if isinstance(date, dict):
            date = date.get("snippet", {}).get("publishTime")
        return AlignmentDataset._get_published_date(date)


@dataclass
class YouTubePlaylistDataset(YouTubeDataset):

    playlist_ids: List[str]

    @property
    def collection_ids(self):
        return self.playlist_ids

    def next_page(self, collection_id: str, next_page_token: list):
        return (
            self.youtube.playlistItems()
            .list(
                part="snippet",
                playlistId=collection_id,
                maxResults=50,
                pageToken=next_page_token,
            )
            .execute()
        )

    @staticmethod
    def _extract_published_date(video) -> str | None:
        return video.get("snippet", {}).get("publishedAt")

    @staticmethod
    def _get_published_date(date) -> datetime | None:  # type: ignore[override]
        if isinstance(date, dict):
            date = date.get("snippet", {}).get("publishedAt")
        return AlignmentDataset._get_published_date(date)
