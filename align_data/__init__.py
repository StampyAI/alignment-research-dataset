import align_data.sources.arbital as arbital
import align_data.sources.articles as articles
import align_data.sources.blogs as blogs
import align_data.sources.ebooks as ebooks
import align_data.sources.arxiv_papers as arxiv_papers
import align_data.sources.greaterwrong as greaterwrong
import align_data.sources.stampy as stampy
import align_data.sources.alignment_newsletter as alignment_newsletter
import align_data.sources.distill as distill
import align_data.sources.youtube as youtube

DATASET_REGISTRY = (
    arbital.ARBITAL_REGISTRY
    + articles.ARTICLES_REGISTRY
    + blogs.BLOG_REGISTRY
    + ebooks.EBOOK_REGISTRY
    + arxiv_papers.ARXIV_REGISTRY
    + greaterwrong.GREATERWRONG_REGISTRY
    + stampy.STAMPY_REGISTRY
    + distill.DISTILL_REGISTRY
    + alignment_newsletter.ALIGNMENT_NEWSLETTER_REGISTRY
    + youtube.YOUTUBE_REGISTRY
)

ALL_DATASETS = sorted([dataset.name for dataset in DATASET_REGISTRY])
DATASET_MAP = {dataset.name: dataset for dataset in DATASET_REGISTRY}


def get_dataset(name):
    try:
        return DATASET_MAP[name]
    except KeyError as e:
        print("Available datasets:")
        print(ALL_DATASETS)
        raise KeyError(f"Missing dataset {name}")
