from align_data.common.alignment_dataset import MultiDataset
from align_data.sources.youtube.youtube import (
    YouTubeChannelDataset,
    YouTubePlaylistDataset,
)

YOUTUBE_DATASETS = [
    YouTubeChannelDataset(
        name="rob_miles_ai_safety",
        channel_id="UCLB7AzTwc6VFZrBsO2ucBMg",
        authors=["Rob Miles"],
    ),
    YouTubeChannelDataset(
        name="ai_safety_talks",
        channel_id="UCXowyqjXvFS-tMKF1GwhpkA",
        authors=["Evan Hubinger"],
    ),
    YouTubeChannelDataset(
        name="ai_safety_reading_group",
        channel_id="UC-C23F-9rK2gtRiJZMWsTzQ",
        authors=[],
    ),
    YouTubeChannelDataset(
        name="ai_tech_tu_delft",
        channel_id="UCPK-Ell2WYxyfP5UYzRzjAA",
        authors=[],
    ),
    YouTubeChannelDataset(
        name="ai_explained",
        channel_id="UCNJ1Ymd5yFuUPtn21xtRbbw",
        authors=[],
    ),
    YouTubePlaylistDataset(
        name="ai_alignment_playlist",
        playlist_ids=[
            "PLqYmG7hTraZCRwoyGxvQkqVrZgDQi4m-5",
            "PLqYmG7hTraZBiUr6_Qf8YTS2Oqy3OGZEj",
            "PLAPVC5uNprwY0q4_nyeeHqIT07wZqwjGO",
            "PLCRVRLd2RhZTpdUdEzJjo3qhmX3y3skWA",
            "PLTYHZYmxohXpn5uf8JZ2OouB1PsDJAk-x",
        ],
    ),
]


YOUTUBE_REGISTRY = [
    MultiDataset(name="youtube", datasets=YOUTUBE_DATASETS),
]
