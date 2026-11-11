# AI Alignment Research Dataset

The AI Alignment Research Dataset is a collection of documents related to AI Alignment and Safety from various books, research papers, and alignment related blog posts. This is a work in progress. Components are still undergoing a cleaning process to be updated more regularly.

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

Other columns (`karma`, `tags`, `abstract`, `doi`…) are only filled in for some sources and are empty elsewhere, so every source shares the same columns.

## Usage

Execute the following code to download and parse the files:

```python
from datasets import load_dataset
data = load_dataset('StampyAI/alignment-research-dataset')
```

To only get the data for a specific source, pass it in as the second argument, e.g.:

```python
from datasets import load_dataset
data = load_dataset('StampyAI/alignment-research-dataset', 'lesswrong')
```

## Changes since the November 2023 version

- The data is refreshed from the current ARD database. The previous upload was from November 2023, and `blogs`, `youtube` and `agisf` dated from August 2023.
- Only rows that pass the ARD's validity checks are included.
- Every source now has the same columns, so the `all` config and the per-source configs share one schema.
- `summary` is now filled in where a summary exists. It was always empty before.
- The raw jsonl files follow the same schema: `summaries` became `summary`, and keys outside the schema (such as `comments` and `bibliography`) are dropped.
- The loading script is gone. `load_dataset` reads the files directly and no longer runs remote code.
- The licence field changed from MIT to "other". See below.

## Licence

The texts belong to their original authors and publishers, and each stays under its own licence and terms. This dataset gathers publicly available writing for research on AI alignment; check the original source before reusing a text elsewhere. The MIT licence covers the [scraper code](https://github.com/StampyAI/alignment-research-dataset), not the texts.

If you are an author and want something removed, please [open an issue](https://github.com/StampyAI/alignment-research-dataset/issues).

## Limitations and Bias

LessWrong posts have overweighted content on doom and existential risk, so please beware in training or finetuning generative language models on the dataset.

## Contributing

The scraper to generate this dataset is open-sourced on [GitHub](https://github.com/StampyAI/alignment-research-dataset) and currently maintained by volunteers at StampyAI / AI Safety Info. [Learn more](https://coda.io/d/AI-Safety-Info_dfau7sl2hmG/Get-involved_susRF#_lufSr) or join us on [Discord](https://discord.gg/vjFSCDyMCy).

## Citing the Dataset

For more information, here is the [paper](https://arxiv.org/abs/2206.02841) and [LessWrong](https://www.lesswrong.com/posts/FgjcHiWvADgsocE34/a-descriptive-not-prescriptive-overview-of-current-ai) post. Please use the following citation when using the dataset:

Kirchner, J. H., Smith, L., Thibodeau, J., McDonnell, K., and Reynolds, L. "Understanding AI alignment research: A Systematic Analysis." arXiv preprint arXiv:2022.4338861 (2022).
