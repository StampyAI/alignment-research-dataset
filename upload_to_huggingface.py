#!/usr/bin/env python3
import sys
from pathlib import Path
from huggingface_hub import login
from huggingface_hub import HfApi


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Usage: python upload_to_huggingface <token> <file>')
    login(sys.argv[1])

    filename = Path(sys.argv[2])

    api = HfApi()
    print(f'Uploading {filename} as {filename.name}')
    api.upload_file(
        path_or_fileobj=filename,
        path_in_repo=filename.name,
        repo_id='StampyAI/alignment-research-dataset',
        repo_type='dataset'
    )
    print('done')
