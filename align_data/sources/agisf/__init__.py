from align_data.common.alignment_dataset import MultiDataset
from align_data.sources.airtable import AirtableDataset
from align_data.sources.agisf.agisf import AGISFPodcastDataset


datasets = [
    AirtableDataset(
        name="agisf_governance",
        base_id="appaD3z5jSZ1NlfAY",
        table_id="tblfwT5injh4IyGYo",
        mappings={
            "title": "[>] Resource",
            "url": "[h] [>] Link",
            "source_type": "[h] [>] Type",
            "comments": "[h] Resource guide",
            "authors": "Author(s) (from Resources)",
        },
        processors={
            "source_type": lambda val: val[0] if val else None,
            "authors": lambda val: val and [v.strip() for v in val.split(",")],
        },
    ),
    AGISFPodcastDataset(
        name="agisf_readings_alignment",
        url="https://feeds.type3.audio/agi-safety-fundamentals--alignment.rss",
    ),
    AGISFPodcastDataset(
        name="agisf_readings_governance",
        url="https://feeds.type3.audio/agi-safety-fundamentals--governance.rss",
    ),
]


AGISF_DATASETS = [
    MultiDataset(name="agisf", datasets=datasets),
]
