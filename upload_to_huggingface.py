#!/usr/bin/env python3
import sys
from pathlib import Path
import requests
import gdown
from gdown.download_folder import _parse_google_drive_file
import jsonlines
from huggingface_hub import login
from huggingface_hub import HfApi

GDOCS_FOLDER = 'https://drive.google.com/drive/folders/1n4i0J4CuSfNmrUkKPyTFKJU0XWYLtRF8'


def upload(api, filename):
    print(f'Uploading {filename} as {filename.name}')
    api.upload_file(
        path_or_fileobj=filename,
        path_in_repo=filename.name,
        repo_id='StampyAI/alignment-research-dataset',
        repo_type='dataset'
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
    return [(id, name) for id, name, filetype in id_name_type_iter if name.endswith('.jsonl')]


if __name__ == "__main__":
    if len(sys.argv) < 2 or not sys.argv[1]:
        print('Usage: python upload_to_huggingface <token> <file>')
        sys.exit(2)
    login(sys.argv[1])
    api = HfApi()

    if len(sys.argv) > 2 and sys.argv[2] != 'all':
        upload(api, Path(sys.argv[2]))
    else:
        data = Path('data/')
        for id, name in get_gdoc_names(GDOCS_FOLDER):
            gdown.download(f'https://drive.google.com/uc?id={id}', str(data / name), quiet=False)
            try:
                # Check that the dowloaded file really contains json lines
                with jsonlines.open(data / name) as reader:
                    reader.read()
            except InvalidLineError as e:
                print(e)
            else:
                upload(api, data / name)

    print('done')
