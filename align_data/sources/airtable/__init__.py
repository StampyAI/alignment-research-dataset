from align_data.common.alignment_dataset import MultiDataset
from align_data.sources.airtable.airtable import AirtableDataset

AGISF_DATASETS = [
    AirtableDataset(
        name='agisf_governance',
        base_id='app9q0E0jlDWlsR0z',
        table_id='tblgTb3kszvSbo2Mb',
        mappings={
            'title': '[>] Resource',
            'url': '[h] [>] Link',
            'source_type': '[h] [>] Type',
            'comments': '[h] Resource guide',
            'authors': 'Author(s) (from Resources)',
        },
        processors = {
            'source_type': lambda val: val[0] if val else None,
            'authors': lambda val: val and [v.strip() for v in val.split(',')]
        }
    ),
]

AIRTABLE_DATASETS = [
    MultiDataset(name='agisf', datasets=AGISF_DATASETS),
]
