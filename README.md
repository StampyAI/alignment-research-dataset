# AI Alignment Research Dataset

The AI Alignment Research Dataset is a collection of documents related to AI Alignment and Safety from various books, research papers, and alignment related blog posts. This is a work in progress. Components are still undergoing a cleaning process to be updated more regularly. The most current version is available on [HuggingFace StampyAI/alignment-research-dataset](https://huggingface.co/datasets/StampyAI/alignment-research-dataset). This repository is the code to reproduce it.

## Sources

Here are the list of sources along with sample contents:

- [agentmodel](https://agentmodels.org/)
- [agisf](https://course.aisafetyfundamentals.com/) - recommended readings from AGI Safety Fundamentals
- [aisafety.info](https://aisafety.info/) - Stampy's FAQ
- [alignmentforum](https://www.alignmentforum.org)
- [alignment_newsletter](https://rohinshah.com/alignment-newsletter/)
- [arbital](https://arbital.com/)
- [arxiv](https://arxiv.org/) - relevant research papers

- blogs - entire websites automatically scraped
  - [AI Impacts](https://aiimpacts.org/)
  - [AI Safety Camp](https://aisafety.camp/)
  - [carado.moe](https://carado.moe/)
  - [Cold Takes](https://www.cold-takes.com/)
  - [DeepMind technical blogs](https://www.deepmind.com/blog-categories/technical-blogs)
  - [DeepMind AI Safety Research](https://deepmindsafetyresearch.medium.com/)
  - [EleutherAI](https://blog.eleuther.ai/)
  - [generative.ink](https://generative.ink/posts/)
  - [Gwern Branwen's blog](https://gwern.net/)
  - [Jack Clark's Import AI](https://importai.substack.com/)
  - [MIRI](https://intelligence.org/)
  - [Jacob Steinhardt's blog](https://jsteinhardt.wordpress.com/)
  - [ML Safety Newsletter](https://newsletter.mlsafety.org/)
  - [Transformer Circuits Thread](https://transformer-circuits.pub/)
  - [Open AI Research](https://openai.com/research/)
  - [Victoria Krakovna's blog](https://vkrakovna.wordpress.com/)
  - [Eliezer Yudkowsky's blog](https://www.yudkowsky.net/)

- [distill](https://distill.pub/)
- [eaforum](https://forum.effectivealtruism.org/) - selected posts
- [lesswrong](https://www.lesswrong.com/) - selected posts

- special_docs - individual documents curated from various resources
  - [Make a suggestion](https://bit.ly/ard-suggestion) for sources not already in the dataset

- youtube - playlists & channels
  - [AI Alignment playlist](https://www.youtube.com/playlist?list=PLCRVRLd2RhZTpdUdEzJjo3qhmX3y3skWA) and other lists
  - [AI Explained](https://www.youtube.com/@aiexplained-official)
  - [Evan Hubinger's AI Safety Talks](https://www.youtube.com/@aisafetytalks)
  - [AI Safety Reading Group](https://www.youtube.com/@aisafetyreadinggroup/videos)
  - [AiTech - TU Delft](https://www.youtube.com/@AiTechTUDelft/)
  - [Rob Miles AI](https://www.youtube.com/@RobertMilesAI)

## Keys

All entries contain the following keys:

- `id` - string of unique identifier
- `source` - string of data source listed above
- `title` - string of document title of document
- `authors` - list of strings
- `text` - full text of document content
- `url` - string of valid link to text content
- `date_published` - in UTC format

Additional keys may be available depending on the source document.

## Development Environment

### 1. Clone the repository:

```sh
git clone https://github.com/StampyAI/alignment-research-dataset
cd alignment-research-dataset
```

### 2. Set up Environment Variables:

Duplicate the provided `.env.example` to create your environment configuration:

```sh
cp .env.example .env
```

This `.env` file contains placeholders for several configuration options. Further details about how to configure them are in the [Configuration section](#configuration).

### 3. Install Dependencies:

```sh
pip install -r requirements.txt
```

**Optional:** For testing purposes, you can also install testing dependencies:

```sh
pip install -r requirements-test.txt
```

### 4. Database Setup:

Initialize a MySQL database. To do so with [Docker](https://docs.docker.com/get-docker/), and spin up a container with the database initialised, run the following:

```sh
./local_db.sh
```

## Configuration

Various subcomponents in this project rely on external services, so need credentials set. This is done via environment variables. The file `.env` is the central location for these settings.

### Logging

The log level can be configured with the `LOG_LEVEL` environment variable. The default level is 'WARNING'.

### Coda

To update the stampy portion of the dataset, you will need a Coda token. Follow these instructions:
    1. Go to [coda.io](https://coda.io/)
    2. Create an account and log in
    3. Go to the API SETTINGS section of your [account settings](https://coda.io/account), and select `Generate API token`. Give your API token a name, and add the following restrictions:
       1. Type of restriction: Doc or table
       2. Type of access: Read only
       3. Doc or table to grant access to: https://coda.io/d/_dfau7sl2hmG
    4. Copy this token to your `.env` file: `CODA_TOKEN="<coda_token>"`
It will be then accessible in `align_data/stampy/stampy.py`.

### MySQL

The datasets are stored in MySQL. The connection string can be configured via the `ARD_DB_USER`,
`ARD_DB_PASSWORD`, `ARD_DB_HOST`, `ARD_DB_PORT` and `ARD_DB_NAME` environment variables in `.env`. A local
database can be started in Docker by running
```sh
./local_db.sh
```

### Pinecone

For Pinecone updates to work, you'll need to configure the API key:

1. Get an API key, as described [here](https://docs.pinecone.io/docs/quickstart#2-get-and-verify-your-pinecone-api-key)
2. Create a Pinecone index named "stampy-chat-ard" (or whatever is set as `PINECONE_INDEX_NAME`) with the `dotproduct` metric and `1536` dimensions
3. Set the `PINECONE_API_KEY` to the key from step 1
4. Set the `PINECONE_ENVIRONMENT` to whatever is the environment of your index


### Google API

To autopopulate the metadata files, you'll need Google Cloud credentials. This is a google system, so of course is complicated and prone to arbitrary changes, but as of writing this the process is:

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing project.
3. Google sheets etc will have to be enabled
    * Enable the Google Sheets API for your project at https://console.cloud.google.com/apis/api/sheets.googleapis.com/metrics?project=<your project id>
    * Enable the Google Drive API for your project at https://console.cloud.google.com/apis/api/drive.googleapis.com/metrics?project=<your project id>
    * Enable the Youtube API for your project at https://console.cloud.google.com/apis/library/youtube.googleapis.com?project=<your project id>. Note that the youtube API is quite limited in the number of requests it can perform.
      An alternative to this step is that when running the program without these enabled, an exception will be raised telling you how to enable it - you can then just open the link in the exception message
4. Navigate to the "Credentials" section, and to `+ Create Credentials`.
5. Select "Service Account"
6. Fill in the required information for the service account:
   1. A descriptive name, a short service account ID, and description. Press `Create and Continue`
   2. Leave the optional sections empty
7. At https://console.cloud.google.com/apis/credentials?project=<your project id>, select your new Service Account, and go to the KEYS section. Select ADD KEY, "Create New Key", the JSON key type and click "Create". 
8. The JSON file containing your credentials will be downloaded. Save it as credentials.json in the top-level directory of the project.
9. Again in the "Credentials" section, `+ Create Credentials`, select API key, and add the created API key as your `YOUTUBE_API_KEY`.

Once you have working credentials, you will be able to fetch data from public sheets and gdrive. For writing to sheets and drives, or accessing private ones within the code, you will need to request permissions to the owner of the particular sheet/gdrive. 

#### Metadata updates

There are a couple of datasources that consist of singular articles (html, pdfs, ebooks, etc), rather than all the contents of a given website. These are managed in [Google sheets](https://docs.google.com/spreadsheets/d/1l3azVJVukGAvZPgg0GyeqiaQe8bEMZvycBJaA8cRXf4/edit#gid=0). It's assumed that the contents of that document are clean, in that all required fields are set, and that there is a `source_url` pointing to a valid document. Rather than having to manually fill these fields, there is a magical script that automatically populates them from a messy [input worksheet](https://docs.google.com/spreadsheets/d/1pgG3HzercOhf4gniaqp3tBc3uvZnHpPhXErwHcthmbI/edit?pli=1#gid=980957638), which contains all kinds of info.

### OpenAI API

1. Go to [the openai api website](https://platform.openai.com/). Create an account if needed, and add payment information if needed.
2. In https://platform.openai.com/account/api-keys, create a new secret key or use a used one.
3. Add this secret key to the `.env`, as `OPENAI_API_KEY`.

### Airtable API

The airtable we currently scrape is https://airtable.com/appbiNKDcn1sGPGOG/shro9Bx4f2i6QgtTM/tblSicSC1u6Ifddrq. #TODO: document how this is done / reproduceable

## CLI Usage

There are various commands available to interact with the datasets:

- **Access the MySQL database in a separate terminal before running most commands:**
    ```sh
    ./local_db.sh
    ```

- **Listing all datasets:**
    ```sh
    python main.py list
    ```

- **Fetching a specific dataset:**
    Replace `[DATASET_NAME]` with the desired dataset. The optional `--rebuild` parameter allows you to remove the previous build before running, scraping everything from scratch. Otherwise, only the new files will be scraped.

    ```sh
    python main.py fetch [DATASET_NAME] --rebuild
    ```

- **Fetching all datasets:**
    Again, the optional `--rebuild` parameter allows you to scrape everything from scratch.
    ```sh
    python main.py fetch-all --rebuild
    ```

- **Getting a summary of a merged dataset:**
    Replace `[MERGED_DATASET_PATH]` with your dataset's path. You'll get access to the dataset's total token count, word count and character count.
    ```sh
    python main.py count-tokens [MERGED_DATASET_PATH]
    ```
  
- **Updating the metadata in the metadata spreadsheet:**
    You can give the command optional information about the names and ids of the sheets, and the default will be using values defined in align_data/settings.py
    ```sh
    python main.py update_metadata
    python main.py update_metadata <input spreadsheet id> <input sheet name> <output spreadsheet id>
    ```

- **Updating the pinecone index with newly modified entries:**
    Replace `[DATASET_NAME]` with one or many dataset names whose entries you want to embed and add to the pinecone index.
    `--force_update` is an optional parameter for updating all the dataset's articles, rather than newly fetched ones.
    ```sh
    python main.py pinecone_update [DATASET_NAME] --force_update
    ```
    Or run it on all articles as seen below. It is not recommended to `--force_update` in this case.
    ```sh
    python main.py pinecone_update_all
    ```

## Adding New Datasets

Adding a new dataset consists of:

1. Subclassing `AlignmentDataset` to implement any additional functionality needed, within align_data/sources/
2. Creating an instance of your class somewhere, such as an __init__.py file (you can take inspiration on other such files)
3. Adding the instance to `DATASET_REGISTRY` so it can be found

### AlignmentDataset class

This is the main workhorse for processing datasets. The basic idea is that it provides a list of items to be processed, and after processing a given item, creates an article object, which is added to the MySQL database. The `AlignmentDataset` class has various methods that can be implemented to handle various cases. A few assumptions are made as to the data it will use, i.e.:

* `self.data_path` is where data will be written to and read from - by default it's the `data/` directory
* `self.raw_data_path` is where downloaded files etc. should go - by default it's the `data/raw` directory
* `self.files_path` is where data to be processed is expected to be. This is used e.g. when a collection of html files are to be processed

The `AlignmentDataset` is a dataclass, so it has a couple of settings that control it:

* `name` - this is a string that identifies the dataset, i.e. 'lesswrong'
* `done_key` - used to check if a given item has already been processed.
* `COOLDOWN` - an optional value of the amount of seconds to wait between processing items - this is useful e.g. when fetching items from an API in order to avoid triggering rate limits

The basic processing flow is:

1. `self.setup()` - any instance level initialization stuff should go here, e.g. fetching zip files with data
2. `self._load_outputted_items()` - goes through articles in the database, loads the value of their `self.done_key`, and outputs a simplified version of these strings using `normalize_url`
3. `self.items_list` - returns a list of items to be processed.
4. `self.fetch_entries()` - for each of the resulting items:

* extract its key, using `self.get_item_key(item)`
* check if its key has already been processed - if so, skip it
* run `self.process_entry(item)` to get an article, which is then yielded
* the article is added to the database if it satisfies some conditions, like being a modification of the previous instance of that article, having the minimal required keys, etc.

### Adding a new instance

There are Datasets defined for various types of data sources - first check if any of them match your use case. If so, it's just a matter of adding a new entry to the `__init__.py` module of the appropriate data source. If not, you'll have to add your own one - use the prexisting ones as examples. Either way, you should end up with an instance of an `AlignmentDataset` subclass added to one of the registries. If you add a new registry, make sure to add it to `align_data.DATASET_REGISTRY`.

## Contributing

The scraper code and dataset are maintained by [StampyAI](http://stampy.ai) / [AI Safety Info](http://aisafety.info). [Learn more](https://coda.io/d/AI-Safety-Info_dfau7sl2hmG/Get-involved_susRF#_lufSr) or join us on [Rob Miles AI Discord server](https://discord.gg/vjFSCDyMCy).

## Citing the Dataset

The code is based on https://github.com/moirage/alignment-research-dataset. You can download version 1.0 of the dataset [here](https://the-eye.eu/public/AI/Alignment/moirage_alignment-research-dataset/). For more information, here is the [paper](https://arxiv.org/abs/2206.02841) and [LessWrong](https://www.lesswrong.com/posts/FgjcHiWvADgsocE34/a-descriptive-not-prescriptive-overview-of-current-ai) post. Please use the following citation when using the dataset:

Kirchner, J. H., Smith, L., Thibodeau, J., McDonnell, K., and Reynolds, L. "Understanding AI alignment research: A Systematic Analysis." arXiv preprint arXiv:2022.4338861 (2022).
