import collections
import logging
from dataclasses import dataclass
from typing import List

from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import (
    NoTranscriptFound,
    VideoUnavailable,
    TranscriptsDisabled,
)

from align_data.settings import YOUTUBE_API_KEY
from align_data.common.alignment_dataset import AlignmentDataset


logger = logging.getLogger(__name__)


class YouTubeDataset(AlignmentDataset):
    done_key = "url"
    batch_size = 1
    # COOLDOWN = 2
    authors = None
    collection_ids = []

    def setup(self):
        super().setup()
        if not YOUTUBE_API_KEY:
            raise ValueError("No YOUTUBE_API_KEY provided!")
        self.youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

    def next_page(self, collection_id, next_page_token):
        return {"items": []}

    @staticmethod
    def _get_id(item):
        if item.get("kind") == "youtube#searchResult":
            resource = item["id"]
        elif item.get("kind") == "youtube#playlistItem":
            resource = item["snippet"]["resourceId"]
        else:
            return None

        if resource["kind"] == "youtube#video":
            return resource["videoId"]

    def fetch_videos(self, collection_id):
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

    def get_item_key(self, item):
        video_id = self._get_id(item)
        return f"https://www.youtube.com/watch?v={video_id}"

    def _get_contents(self, video):
        video_id = self._get_id(video)
        try:
            transcript = (
                YouTubeTranscriptApi.list_transcripts(video_id)
                .find_transcript(["en", "en-GB"])
                .fetch()
            )
            return "\n".join([i["text"] for i in transcript])
        except (NoTranscriptFound, VideoUnavailable):
            return None
        except TranscriptsDisabled:
            logger.error(
                f"Transcripts disabled for https://www.youtube.com/watch?v={video_id} - skipping"
            )
            return None

    def extract_authors(self, video):
        if self.authors:
            return self.authors
        return [video["snippet"]["channelTitle"].strip()]

    def process_entry(self, video):
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

    def _get_published_date(self, video):
        return super()._get_published_date(video["snippet"]["publishTime"])


@dataclass
class YouTubePlaylistDataset(YouTubeDataset):
    playlist_ids: str

    @property
    def collection_ids(self):
        return self.playlist_ids

    def next_page(self, collection_id, next_page_token):
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

    def _get_published_date(self, video):
        return super()._get_published_date(video["snippet"]["publishedAt"])
