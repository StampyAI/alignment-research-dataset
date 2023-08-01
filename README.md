# AI Alignment Research Dataset

The AI Alignment Research Dataset is a collection of documents related to AI Alignment and Safety from various books, research papers, and alignment related blog posts. This is a work in progress. Components are still undergoing a cleaning process to be updated more regularly. The most current version is available on [HuggingFace StampyAI/alignment-research-dataset](https://huggingface.co/datasets/StampyAI/alignment-research-dataset). This repository is the code to reproduce it.

## Sources

The following list of sources may change and items may be renamed:

- [agentmodels](https://agentmodels.org/)
- [aiimpacts](https://aiimpacts.org/)
- [aisafety.camp](https://aisafety.camp/)
- [aisafety.info](https://aisafety.info/)
- [alignmentforum](https://www.alignmentforum.org)
- [alignment_newsletter](https://rohinshah.com/alignment-newsletter/)
- [arbital](https://arbital.com/)
- arxiv - alignment research papers from [arxiv](https://arxiv.org/)
- audio_transcripts - transcripts from interviews with various researchers and other audio recordings
- [carado.moe](https://carado.moe/)
- [cold_takes](https://www.cold-takes.com/)
- [deepmind_blog](https://deepmindsafetyresearch.medium.com/)
- [distill](https://distill.pub/)
- [eaforum](https://forum.effectivealtruism.org/) - selected posts
- ebooks - books include [Superintelligence](https://www.goodreads.com/book/show/20527133-superintelligence), [Human Compatible](https://www.goodreads.com/book/show/44767248-human-compatible), [Life 3.0](https://www.goodreads.com/book/show/34272565-life-3-0), [The Precipice](https://www.goodreads.com/book/show/50485582-the-precipice), and others
- gdocs
- [generative.ink](https://generative.ink/posts/)
- [gwern_blog](https://gwern.net/)
- [import.ai](https://importai.substack.com)
- [jsteinhardt_blog](https://jsteinhardt.wordpress.com/)
- [lesswrong](https://www.lesswrong.com/) - selected posts
- markdown.ebooks
- [miri](https://intelligence.org/) - MIRI
- [ml_safety_newsletter](https://newsletter.mlsafety.org)
- nonarxiv_papers - other alignment research papers
- [qualiacomputing](https://qualiacomputing.com/)
- reports
- [vkrakovna_blog](https://vkrakovna.wordpress.com)
- [waitbutwhy](https://waitbutwhy.com/)
- [yudkowsky_blog](https://www.yudkowsky.net/)

## Keys

Not all of the entries contain the same keys, but they all have the following:

- `id` - unique identifier
- `source` - based on the data source listed in the previous section
- `title` - title of document
- `text` - full text of document content
- `url` - some values may be `'n/a'`, still being updated
- `date_published` - some `'n/a'`

The values of the keys are still being cleaned up for consistency. Additional keys are available depending on the source document.

## Development Environment

To set up the development environment, run the following steps. You'll have to also set up [mysqlclient](https://pypi.org/project/mysqlclient/):

```bash
git clone https://github.com/StampyAI/alignment-research-dataset
cd alignment-research-dataset
pip install -r requirements.txt
```

### Database

You'll also have to set up a MySQL database. To do so with Docker, you can run `./local_db.sh` which should spin up a container
with the database initialised.

### CLI options

The available CLI options are list, fetch, fetch-all, and count-tokens.

To get a list of all available datasets:

```sh
python main.py list
```

To fetch a specific dataset, replace [DATASET_NAME] with the name of the dataset you want to fetch. The optional `--rebuild` parameter allows you to remove the previous build before running, scraping everything from scratch. Otherwise, only the new files will be scraped.

```sh
python main.py fetch [DATASET_NAME] --rebuild
```

The command to fetch all datasets is below. Again, the optional `--rebuild` parameter allows you to scrape everything from scratch.

```sh
python main.py fetch-all --rebuild
```

To get a summary of the merged dataset, Replace [MERGED_DATASET_PATH] with the path to the merged dataset file.

```sh
python main.py count-tokens [MERGED_DATASET_PATH]
```

## New Datasets

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

## Running the code

When wishing to update the whole dataset, run `python main.py fetch_all`. You can also fetch a specific subsection of a dataset by its name, for example `python main.py fetch aisafety.info`.

### Coda

To update the stampy portion of the dataset, you will need a Coda token. go to coda.io, log in, and generate an API token in your account settings. Add restrictions: Doc or table, Read only, for the doc with url https://coda.io/d/_dfau7sl2hmG. Then, create a .env file at the root of the alignment research dataset, and write CODA_TOKEN="<coda_token>". It will be accessible in align_data/stampy/stampy.py

### Metadata updates

There are a couple of datasources that consist of singular articles (html, pdfs, ebooks, etc), rather than all the contents of a given website. These are managed in [Google sheets](https://docs.google.com/spreadsheets/d/1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4/edit#gid=0). It's assumed that the contents of that document are clean, in that all required fields are set, and that there is a `source_url` pointing to a valid document. Rather than having to manually fill these fields, there is a magical script that automatically populates them from a messy [input worksheet](https://docs.google.com/spreadsheets/d/1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI/edit?pli=1#gid=980957638), which contains all kinds of info. The following will execute this script:

    python main.py update_metadata <input spreadsheet id> <input sheet name> <output spreadsheet id>

which for the current documents would be:

    python main.py update_metadata 1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI special_docs.csv 1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4

#### Google API

To autopopulate the metadata files, you'll need Google Cloud credentials. This is a google system, so of course is complicated and prone to arbitrary changes, but as of writing this the process is:

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing project (it doesn't matter either way)
3. Google sheets etc will have to be enabled
 * Enable the Google Sheets API for your project at https://console.cloud.google.com/apis/api/sheets.googleapis.com/metrics?project=<your project id>
 * Enable the Google Drive API for your project at https://console.cloud.google.com/apis/api/drive.googleapis.com/metrics?project=<your project id>
 An alternative to this step is that when running the program without these enabled, an exception will be raised telling you how to enable it - you can then just open the link in the exception message
4. Navigate to the "Credentials" section
5. Click on "Create Credentials" and select "Service Account"
6. Fill in the required information for the service account
7. On the "Create key" page, select the JSON key type and click "Create"
8. The JSON file containing your credentials will be downloaded -> save as credentials.json in the folder from which you're running the code

## Contributing

The scraper code and dataset are maintained by [StampyAI](http://stampy.ai) / [AI Safety Info](http://aisafety.info). [Learn more](https://coda.io/d/AI-Safety-Info_dfau7sl2hmG/Get-involved_susRF#_lufSr) or join us on [Rob Miles AI Discord server](https://discord.gg/vjFSCDyMCy).

## Citing the Dataset

The code is based on https://github.com/moirage/alignment-research-dataset. You can download version 1.0 of the dataset [here](https://the-eye.eu/public/AI/Alignment/moirage_alignment-research-dataset/). For more information, here is the [paper](https://arxiv.org/abs/2206.02841) and [LessWrong](https://www.lesswrong.com/posts/FgjcHiWvADgsocE34/a-descriptive-not-prescriptive-overview-of-current-ai) post. Please use the following citation when using the dataset:

Kirchner, J. H., Smith, L., Thibodeau, J., McDonnell, K., and Reynolds, L. "Understanding AI alignment research: A Systematic Analysis." arXiv preprint arXiv:2022.4338861 (2022).
