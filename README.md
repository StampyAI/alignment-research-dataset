# AI Alignment Research Dataset

A dataset of alignment research and code to reproduce it. The most current version of the dataset is available on [HuggingFace StampyAI/alignment-research-dataset](https://huggingface.co/datasets/StampyAI/alignment-research-dataset)

## Sources

Below, you can find a table of the number of texts in the dataset grouped into various sources. The table is up-to-date with version 1.0 of the dataset (June 4th, 2022).

<img src="./imgs/dataset_sources.PNG" alt="dataset_sources.PNG" width=600 />

## Development Environment

To set up the development environment, run the following steps:

```bash
git clone https://github.com/moirage/alignment-research-dataset
cd alignment-research-dataset
pip install -r requirements.txt
```

To get a list of all available datasets:

```bash
python main.py list
```

To scrape an individual dataset:

```bash
python main.py fetch -d {dataset}
```

You will also need do install [grobid](https://github.com/kermitt2/grobid) on your machine to run some of the scripts. There is some documentation [here](https://grobid.readthedocs.io/en/latest/Install-Grobid/) on how to install it. The config.json file in the root of this repository is for grobid.

## Using the Dataset

The dataset is in .jsonl format. Each line is a new entry for that dataset. To load the dataset, you can use the `jsonlines` python package. You can load the dataset using the following:

```python
import jsonlines

dictionary = {}
with jsonlines.open("alignment_texts.jsonl", "r") as reader:
  for entry in reader:
    try:
      # grab contents of each entry here, example:
      # dictionary[i]['text'] = entry['text']
    except KeyError:
      pass
```

### What Keys are in Each JSON of the Dataset?

The important thing here is that not all of the dataset entries contain all the same keys (though they all have the `text` key). That said, the key names are standardized so you should not run into any issues where `source` in one entry is something like `source_of_entry` in another. We do this because it doens't make sense to say "journal_ref" when we are talking about an audio transcript. So, you will need to make sure you add a `try-except` in your code if you want to grab things other than `text`.

For now, if you would like to know the specific keys from each source in the dataset, please look at the code for that source in [align_data](./align_data).

Here's what the data for the arXiv papers looks like:

```json
{
"source": "arxiv", # where the dataset comes from
"source_type": "latex", # the type of file the data was original in
"converted_with": "pandoc", # which tool we used to convert the data in .md format
"paper_version": paper_id,
"title": title,
"authors": [str(x) for x in authors], # list of authors
"date_published": date_published,
"data_last_modified": data_last_modified,
"url": url,
"abstract": abstract,
"author_comment": author_comment,
"journal_ref": journal_ref,
"doi": doi,
"primary_category": primary_category,
"categories": categories,
"citation_level": citation_level, # (0 = curated alignment papers, 1 = citation of curated papers, 2 = citation of citation, etc.)
"alignment_text": is_alignment_text, # 'pos' is maunally labeled as an alignment paper, 'unlabeled' if unlabeled
"confidence_score": confidence_scores, # this is a confidence score obtained by using the SPECTER model to classify papers to add to the dataset
"main_tex_filename": "main.tex", # the main latex file needed to convert the paper
"text": "lots of text", # this is where you will grab the text contents of each entry in the dataset (in .md format)
"bibliography_bbl": "string of bbl",
"bibliography_bib": "string of bib", # more common to have bib than bbl
}
```

### The 80/20 for using the dataset

As we said in the previous section, all entries have the `text` key which contains the text content for that entry. Here's some other common keys you might use:

1. `source`: this key separates the various keys found in the table in [Sources](##Sources). Here's the set of sources with their corresponding value name:

* agentmodels (21 rows)
* alimpacts.org (235 rows)
* aipulse.org (23 rows)
* aisafety.camp (9 rows)
* arbital (732 rows)
* arxiv_papers (829 rows)
* audio_transcripts (37 rows)
* carado.moe (59 rows)
* cold.takes (91 rows)
* deepmind.blog (10 rows)
* distill (45 rows)
* eaforum (12.4k rows)
* gdocs gdrive_ebooks
* generative.ink (18 rows)
* gwern_blog (7 rows)
* intelligence.org (483 rows)
* jsteinhardt.wordpress.com (39 rows)
* lesswrong (6.23k rows)
* markdown.ebooks (4 rows)
* nonarxiv_papers (198 rows)
* qualiacomputing.com (289 rows)
* reports (55 rows)
* stampy (198 rows)
* vkrakovna.wordpress.com (43 rows)
* waitbutwhy (2 rows)
* www.yudkowsky.net (23 rows)

## Adding new datasets

Adding a new dataset consists of:

1. Subclassing `AlignmentDataset` to implement any additional functionality needed
2. Creating an instance of your class somewhere
3. Adding the instance to `DATASET_REGISTRY` so it can be found

### AlignmentDataset class

This is the main workhorse for processing datasets. The basic idea is that it provided a list of items to be processed, and after processing a given item, appends it to the appropriate jsonl file, where each line of the file is a JSON object with all the data. The `AlignmentDataset` class has various methods that can be implemented to handle various cases. A few assumptions are made as to the data it will use, i.e.:

* `self.data_path` is where data will be written to and read from - by default it's the `data/` directory
* `self.raw_data_path` is where downloaded files etc. should go - by default it's the `data/raw` directory
* `self.files_path` is where data to be processed is expected to be. This is used e.g. when a collection of html files are to be processed
* `self.jsonl_path` is the path to the output JSONL file, by default `data/<name>.jsonl`
* `self.txt_path` is the path to the debug file

The `AlignmentDataset` is a dataclass, so it has a couple of settings that control it:

* `name` - this is a string that identifies the dataset, i.e. 'lesswrong'
* `done_key` - used to check if a given item has already been processed. This is a key in the JSON object that gets written to the output file - any subsequent entries with the same value for that key will be skipped
* `glob` - a glob used to select files from the `self.files_path` - this controls what files are processed
* `COOLDOWN` - an optional value of the amount of seconds to wait between processing items - this is useful e.g. when fetching items from an API in order to avoid triggering rate limits

The basic processing flow is:

1. `self.setup()` - any instance level initialization stuff should go here, e.g. fetching zip files with data
2. `self._load_outputted_items()` - go through `self.jsonl_path` and construct a set of the `self.done_key` values of each item - this is used to skip items that have already been processed
3. `self.items_list` - returns a list of items to be processed - the default is to use `self.glob` on `self.files_path`
4. `self.fetch_entries()` - for each of the resulting items:

* extract its key, using `self.get_item_key(item)`
* check if its key has already been processed - if so, skip it
* run `self.process_entry(item)` to get a data entry, which is then yielded
* the data entry is written to `self.jsonl_path`

### Adding a new instance

There are Datasets defined for various types of data sources - first check if any of them match your use case. If so, it's just a matter of adding a new entry to the `__init__.py` module of the appropriate data source. If not, you'll have to add your own one - use the prexisting ones as examples. Either way, you should end up with an instance of an `AlignmentDataset` subclass added to one of the registries. If you add a new registry, make sure to add it to `align_data.DATASET_REGISTRY`.

## Contributing

Join us on [Rob Mile's discord server](https://discord.com/invite/7wjJbFJnSN) in the #stampy-dev channel.

## Citing the Dataset

The code is based on https://github.com/moirage/alignment-research-dataset. You can download version 1.0 of the dataset [here](https://the-eye.eu/public/AI/Alignment/moirage_alignment-research-dataset/). For more information, here is the [paper](https://arxiv.org/abs/2206.02841) and [LessWrong](https://www.lesswrong.com/posts/FgjcHiWvADgsocE34/a-descriptive-not-prescriptive-overview-of-current-ai) post. Please use the following citation when using the dataset:

Kirchner, J. H., Smith, L., Thibodeau, J., McDonnell, K., and Reynolds, L. "Understanding AI alignment research: A Systematic Analysis." arXiv preprint arXiv:2022.4338861 (2022).
