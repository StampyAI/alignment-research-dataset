#!/usr/bin/env python3
"""Upload the jsonl files made by `python main.py generate_jsonl_files` to Hugging Face.

Usage: HF_TOKEN=<token> python upload_to_huggingface.py [<dataset name> | all] [--dry-run]

With --dry-run, the files that would be uploaded (including the dataset card) are written to
data/huggingface/ instead, so they can be checked with `datasets.load_dataset("data/huggingface")`.
If HF_TOKEN is set, the dry run also prints who the token belongs to and its rights.
"""
import json
import os
import sys
from pathlib import Path

import jsonlines
import yaml
from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi

from align_data import ALL_DATASETS

REPO_ID = "StampyAI/alignment-research-dataset"
DATA_DIR = Path("data")
OUT_DIR = DATA_DIR / "huggingface"
CARD_BODY = Path(__file__).parent / "huggingface_card.md"
# The old loading script. Recent `datasets` releases no longer run loading scripts, so the
# configs are now declared in the card instead.
OBSOLETE_FILES = ["alignment-research-dataset.py"]

# Commercial books from the special_docs "ebooks" sheet. We have no permission to republish them,
# so they're never exported (see #262). "Smarter Than Us" is left in: MIRI publishes it free.
EXCLUDED_URLS = {
    "https://www.goodreads.com/book/show/698866.The_Myth_of_the_Rational_Voter",
    "https://www.goodreads.com/book/show/2659696-global-catastrophic-risks",
    "https://www.goodreads.com/en/book/show/44154569",  # The AI Does Not Hate You
    "https://www.goodreads.com/book/show/34272565-life-3-0",
    "https://www.goodreads.com/book/show/50489349-the-alignment-problem",
    "https://www.goodreads.com/book/show/32763109-2016-international-symposium-on-experimental-robotics",
    "https://www.goodreads.com/book/show/27543.Artificial_Intelligence",  # A Modern Approach
    "https://www.goodreads.com/book/show/39204073-on-the-future",
    "https://www.goodreads.com/book/show/34540069-robot-sex",
    "https://www.goodreads.com/book/show/26831944-the-age-of-em",
    "https://link.springer.com/book/10.1007/978-3-662-54033-6",  # The Technological Singularity: Managing the Journey
    "https://www.goodreads.com/book/show/20527133-superintelligence",
    "https://www.goodreads.com/book/show/55605905-the-technological-singularity",
    "https://www.goodreads.com/book/show/38471807-solomon-s-code",
    "https://www.goodreads.com/book/show/48570420-the-precipice",
    "https://www.goodreads.com/book/show/42283306-end-times",
    "https://www.goodreads.com/book/show/23316526-the-second-machine-age",
    "https://www.goodreads.com/book/show/44767248-human-compatible",
    "https://www.goodreads.com/book/show/53730358-a-citizen-s-guide-to-artificial-intelligence",
    "https://www.goodreads.com/book/show/30528889-fundamental-issues-of-artificial-intelligence",
}

# Every row gets exactly these columns, so all files share one schema. Other keys are dropped
# (e.g. `comments`, which is arbitrarily nested).
FEATURES = [
    {"name": "id", "dtype": "string"},
    {"name": "source", "dtype": "string"},
    {"name": "title", "dtype": "string"},
    {"name": "text", "dtype": "large_string"},
    {"name": "url", "dtype": "string"},
    {"name": "date_published", "dtype": "string"},
    {"name": "authors", "sequence": "string"},
    {"name": "summary", "sequence": "string"},
    {"name": "source_type", "dtype": "string"},
    {"name": "book_title", "dtype": "string"},
    {"name": "karma", "dtype": "int32"},
    {"name": "votes", "dtype": "int32"},
    {"name": "words", "dtype": "int32"},
    {"name": "comment_count", "dtype": "int32"},
    {"name": "tags", "sequence": "string"},
    {"name": "modified_at", "dtype": "string"},
    {"name": "alias", "dtype": "string"},
    {"name": "data_last_modified", "dtype": "string"},
    {"name": "abstract", "dtype": "string"},
    {"name": "author_comment", "dtype": "string"},
    {"name": "journal_ref", "dtype": "string"},
    {"name": "doi", "dtype": "string"},
    {"name": "primary_category", "dtype": "string"},
    {"name": "categories", "sequence": "string"},
    {"name": "initial_source", "dtype": "string"},
    {"name": "bibliography_bib", "list": [{"name": "title", "dtype": "string"}]},
]


def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_str_list(value):
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value]


def normalize(row):
    """Map a row from `Article.to_dict()` onto the shared schema."""
    out = {}
    for feature in FEATURES:
        name = feature["name"]
        value = row.get("summaries") if name == "summary" else row.get(name)
        if name == "bibliography_bib":
            value = [{"title": b.get("title")} for b in value or [] if isinstance(b, dict)]
        elif "sequence" in feature or "list" in feature:
            value = to_str_list(value)
        elif feature.get("dtype") == "int32":
            value = to_int(value)
        elif value is not None and not isinstance(value, str):
            value = json.dumps(value)
        out[name] = value
    return out


def write_normalized(name):
    """Write a normalized copy of data/<name>.jsonl to OUT_DIR, returning its path and row count."""
    source, target = DATA_DIR / f"{name}.jsonl", OUT_DIR / f"{name}.jsonl"
    seen, count = set(), 0
    with jsonlines.open(source) as reader, jsonlines.open(target, "w") as writer:
        for row in reader:
            if not row.get("id") or row["id"] in seen or row.get("text") in (None, "", "n/a"):
                continue
            if row.get("url") in EXCLUDED_URLS:
                continue
            seen.add(row["id"])
            writer.write(normalize(row))
            count += 1
    return target, count


def make_card(names):
    """Build the dataset card: generated YAML metadata followed by `huggingface_card.md`."""
    configs = [{"config_name": "all", "data_files": [f"{n}.jsonl" for n in names], "default": True}]
    configs += [{"config_name": n, "data_files": f"{n}.jsonl"} for n in names]
    metadata = {
        "language": ["en"],
        "license": "other",
        "license_name": "various",
        "size_categories": ["10K<n<100K"],
        "task_categories": ["question-answering"],
        "pretty_name": "alignment-research-dataset",
        "configs": configs,
        "dataset_info": [{"config_name": c["config_name"], "features": FEATURES} for c in configs],
    }
    header = yaml.safe_dump(metadata, sort_keys=False, allow_unicode=True)
    return f"---\n{header}---\n\n{CARD_BODY.read_text()}"


def check_token(api):
    """Print who the token belongs to and its rights on the org, without changing anything."""
    info = api.whoami()
    role = info.get("auth", {}).get("accessToken", {}).get("role")
    org_roles = {org["name"]: org.get("roleInOrg") for org in info.get("orgs", [])}
    org = REPO_ID.split("/")[0]
    print(f"HF token: user {info['name']}, token role {role}, {org} role {org_roles.get(org)}")


def main(selected, dry_run):
    names = ALL_DATASETS if selected == "all" else [selected]
    missing = [n for n in names if n not in ALL_DATASETS or not (DATA_DIR / f"{n}.jsonl").exists()]
    if missing:
        sys.exit(
            f"Missing data files for {missing}. Run `python main.py generate_jsonl_files` first"
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    uploads = []
    for name in names:
        path, count = write_normalized(name)
        print(f"{path}: {count} rows")
        if count:
            uploads.append(path)
        else:  # e.g. alignment_newsletter, which only adds summaries to other articles
            path.unlink()
            print(f"Skipping {name}: no rows")
    names = [p.stem for p in uploads]

    if dry_run:
        if os.environ.get("HF_TOKEN"):
            check_token(HfApi())
        local = [n for n in ALL_DATASETS if (OUT_DIR / f"{n}.jsonl").exists()]
        (OUT_DIR / "README.md").write_text(make_card(local))
        print(f"Dry run: files written to {OUT_DIR}")
        return

    api = HfApi()  # reads the token from HF_TOKEN
    remote_files = set(api.list_repo_files(REPO_ID, repo_type="dataset"))
    if not names:
        sys.exit("Nothing to upload")
    if selected != "all" and any(f in remote_files for f in OBSOLETE_FILES):
        # The other files on HF still have the 2023 schema, which the new card would contradict
        sys.exit("The first upload in the new format must be `all`")
    # The card lists every dataset that will be on HF after this commit, not just this run's
    card_names = [n for n in ALL_DATASETS if n in names or f"{n}.jsonl" in remote_files]

    operations = [CommitOperationAdd(path_in_repo=p.name, path_or_fileobj=p) for p in uploads]
    operations.append(
        CommitOperationAdd(path_in_repo="README.md", path_or_fileobj=make_card(card_names).encode())
    )
    operations += [
        CommitOperationDelete(path_in_repo=f) for f in OBSOLETE_FILES if f in remote_files
    ]

    commit = api.create_commit(
        repo_id=REPO_ID,
        repo_type="dataset",
        operations=operations,
        commit_message=f"Update {', '.join(names)} from the ARD database",
    )
    print(f"Uploaded: {commit.commit_url}")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    main(args[0] if args else "all", dry_run="--dry-run" in sys.argv)
