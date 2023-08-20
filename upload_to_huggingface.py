#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path
import requests
import gdown
from gdown.download_folder import _parse_google_drive_file
import jsonlines
from huggingface_hub import login
from huggingface_hub import HfApi


GDOCS_FOLDER = (
    "https://drive.google.com/drive/folders/1n4i0J4CuSfNmrUkKPyTFKJU0XWYLtRF8"
)
DATASOURCES = [
    'agentmodels',
    'aiimpacts',
    'aisafety.camp',
    'aisafety.info',
    'ai_alignment_playlist',
    'ai_explained',
    'ai_safety_talks',
    'ai_safety_reading_group',
    'ai_tech_tu_delft',
    'alignmentforum',
    'arbital',
    'arxiv',
    'carado.moe',
    'cold_takes',
    'deepmind_blog',
    'deepmind_technical_blog',
    'distill',
    'eaforum',
    'eleuther.ai',
    'generative.ink',
    'gwern_blog',
    'importai',
    'jsteinhardt_blog',
    'lesswrong',
    'miri',
    'ml_safety_newsletter',
    'openai.research',
    'rob_miles_ai_safety',
    'special_docs',
    'vkrakovna_blog',
    'yudkowsky_blog'
]


def upload(api, filename, repo_name):
    print(f"Uploading {filename} as {repo_name}/{filename.name}")
    api.upload_file(
        path_or_fileobj=filename,
        path_in_repo=filename.name,
        repo_id=f"StampyAI/{repo_name}",
        repo_type="dataset",
    )


def get_gdoc_names(url):
    if "?" in url:
        url += "&hl=en"
    else:
        url += "?hl=en"

    res = requests.get(url)

    if res.status_code != 200:
        return None

    _, id_name_type_iter = _parse_google_drive_file(url=url, content=res.text)
    return [
        (id, name)
        for id, name, filetype in id_name_type_iter
        if name.endswith(".jsonl")
    ]


def upload_data_file(api, name, repo_name):
    """Upload the file with the given `name` to HF."""
    data = Path("data/")
    filename = data / name

    # Don't download it if it exists locally
    if not filename.exists():
        print(f'{filename} not found!')
        return

    try:
        # Check that the dowloaded file really contains json lines
        with jsonlines.open(filename) as reader:
            reader.read()
    except (jsonlines.InvalidLineError, EOFError) as e:
        print(e)
    else:
        upload(api, filename, repo_name)


def download_file(repo_name, filename, api):
    headers = {"Authorization": f"Bearer {api.token}"}
    url = (
        f"https://huggingface.co/datasets/StampyAI/{repo_name}/raw/main/{filename.name}"
    )
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(response.content)


def update_readme(api, files, repo_name):
    """Update the HuggingFace README with the new metadata.

    Huggingface doesn't seem to provide a nice way of updating the README metadata, hence this
    mucking around.
    """
    # Pretend to create the repo locally
    repo = Path(repo_name)
    repo.mkdir(exist_ok=True)

    # Fetch the current README and dataset script
    for filename in ["README.md", f"{repo_name}.py"]:
        download_file(repo_name, repo / filename, api)

    # Copy over all jsonl files that have been updated, and update the README to have the
    # current metadata
    for filename in files:
        target = Path("data") / filename
        (repo / filename).write_text(target.read_text())
        output = subprocess.check_output(
            ["datasets-cli", "test", repo_name, "--save_info", f"--name={target.stem}"]
        )

    # Now upload the updated README
    upload(api, repo / "README.md", repo_name)


if __name__ == "__main__":
    if len(sys.argv) < 2 or not sys.argv[1]:
        print("Usage: python upload_to_huggingface <token> <datasource name | all>")
        sys.exit(2)

    token = sys.argv[1]
    # login(sys.argv[1])
    api = HfApi(token=token)

    files = DATASOURCES
    if len(sys.argv) > 2 and sys.argv[2] != "all":
        files = [item for item in files if item == sys.argv[2]]

    data = Path("data/")
    for name in files:
        upload_data_file(api, name + ".jsonl", "alignment-research-dataset")

    update_readme(
        api,
        [name for _, name in files if name in DATASOURCES],
        "alignment-research-dataset",
    )
    update_readme(api, [name for _, name in files], "ard-private")

    print("done")
