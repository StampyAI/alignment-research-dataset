from align_data.common.alignment_dataset import MultiDataset
from align_data.sources.airtable import AirtableDataset
from align_data.sources.agisf.agisf import AGISFPodcastDataset


datasets = [
    AirtableDataset(
        name="agisf_governance",
        base_id="app9q0E0jlDWlsR0z",
        table_id="tblgTb3kszvSbo2Mb",
        mappings={
            "title": "[>] Resource",
            "url": "[h] [>] Link",
            "source_type": "[h] [>] Type",
            "comments": "[h] Resource guide",
            "authors": "Author(s) (from Resources)",
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
