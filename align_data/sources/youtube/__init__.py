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
    YouTubeChannelDataset(
        name="machine_intelligence_research_institute",
        channel_id="UCutigUZUpKrIMOte27SDmEg",
        authors=[],
    ),
    YouTubeChannelDataset(
        name="agent_foundations_for_ai_alignment_workshop",
        channel_id="UCWYnqb7XeahrMdUvW56TJ1Q",
        authors=[],
    ),
    YouTubeChannelDataset(
        name="neel_nanda",
        channel_id="UCBMJ0D-omcRay8dh4QT0doQ",
        authors=["Neel Nanda"],
    ),
    YouTubeChannelDataset(
        name="future_of_life_institute",
        channel_id="UC-rCCy3FQ-GItDimSR9lhzw",
        authors=[],
    ),
    YouTubeChannelDataset(
        name="the_inside_view",
        channel_id="UCb9F9_uV24PGj6x63PhXEVw",
        authors=[],
    ),
    YouTubeChannelDataset(
        name="center_for_ai_safety",
        channel_id="UCY_K5gXsXHtuiP8mj3BiWxA",
        authors=["Dan Hendrycks"],
    ),
    YouTubeChannelDataset(
        name="alignment_workshop",
        channel_id="UCCV6kbjBZje3LPxRp0NHfxg",
        authors=[],
    ),
    YouTubeChannelDataset(
        name="rational_animations",
        channel_id="UCgqt1RE0k0MIr0LoyJRy2lg",
        authors=[],
    ),
    YouTubePlaylistDataset(
        name="ai_alignment_playlist",
        playlist_ids=[
            "PLqYmG7hTraZCRwoyGxvQkqVrZgDQi4m-5",
            "PLqYmG7hTraZBiUr6_Qf8YTS2Oqy3OGZEj",
            "PLCRVRLd2RhZTpdUdEzJjo3qhmX3y3skWA",
            "PLTYHZYmxohXpn5uf8JZ2OouB1PsDJAk-x",
            "PLWQikawCP4UEiXxzfA8CfBsgDeXzXt9s9",
            "PLoROMvodv4rNtnS3JSRRZzLWQo2dd6XNs",
        ],
    ),
]


YOUTUBE_REGISTRY = [
    MultiDataset(name="youtube", datasets=YOUTUBE_DATASETS),
]
