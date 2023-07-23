from datetime import datetime, timezone
import pytest
import pandas as pd

from align_data.alignment_newsletter import AlignmentNewsletter


@pytest.fixture(scope="module")
def dataset():
    dataset = AlignmentNewsletter(name='text')
    dataset.setup()
    return dataset


def test_xlsx_file_loaded(dataset):
    assert len(dataset.df) == 1956


def test_get_item_key(dataset):
    items = list(dataset.items_list)

    assert dataset.get_item_key(items[0]) == 'Adversarial Examples Are Not Bugs, They Are Features'


def test_process_entry_no_summary(dataset):
    items = pd.DataFrame([
        {'Title': 'An item without a summary field'},
        {'Title': 'An item with a None summary field', 'Summary': None},
        {'Title': 'An item with an invalid summary field', 'Summary': pd.NA},
    ])
    for item in items.itertuples():
        assert dataset.process_entry(item) is None


def test_format_datatime(dataset):
    assert dataset._get_published_date(2022) == datetime(2022, 1, 1, tzinfo=timezone.utc)


def test_process_entry(dataset):
    # Do a basic sanity test of the output. If this starts failing and is too much
    # of a bother to keep up to date, then it can be deleted
    items = list(dataset.items_list)
    assert dataset.process_entry(items[0]).to_dict() == {
        'authors': ['Andrew Ilyas*',
                    'Shibani Santurkar*',
                    'Dimitris Tsipras*',
                    'Logan Engstrom*',
                    'Brandon Tran',
                    'Aleksander Madry'],
        'converted_with': 'python',
        'date_published': '2019-01-01T00:00:00Z',
        'highlight': True,
        'id': None,
        'newsletter_category': 'Adversarial examples',
        'newsletter_number': 'AN #62',
        'opinion': (
            'I buy this hypothesis. It explains why adversarial examples occur '
            '("because they are useful to reduce loss"), and why they transfer '
            'across models ("because different models can learn the same '
            'non-robust features"). In fact, the paper shows that '
            'architectures that did worse in ExpWrongLabels (and so presumably '
            'are bad at learning non-robust features) are also the ones to '
            "which adversarial examples transfer the least. I'll leave the "
            'rest of my opinion to the opinions on the responses.'
        ),
        'prerequisites': '',
        'read_more': '[Paper](https://arxiv.org/abs/1905.02175) and [Author response](https://distill.pub/2019/advex-bugs-discussion/original-authors/)',
        'source': 'text',
        'source_type': 'google-sheets',
        'summarizer': 'Rohin',
        'summaries': [],
        'text': (
            '_Distill published a discussion of this paper. This highlights '
            'section will cover the full discussion; all of these summaries and '
            'opinions are meant to be read together._\n'
            '\n'
            'Consider two possible explanations of adversarial examples. First, '
            'they could be caused because the model "hallucinates" a signal that '
            'is not useful for classification, and it becomes very sensitive to '
            'this feature. We could call these "bugs", since they don\'t '
            'generalize well. Second, they could be caused by features that _do_ '
            'generalize to the test set, but _can_ be modified by an adversarial '
            'perturbation. We could call these "non-robust features" (as opposed '
            'to "robust features", which can\'t be changed by an adversarial '
            'perturbation). The authors argue that at least some adversarial '
            'perturbations fall into the second category of being informative but '
            'sensitive features, based on two experiments.\n'
            '\n'
            'If the "hallucination" explanation were true, the hallucinations '
            'would presumably be caused by the training process, the choice of '
            'architecture, the size of the dataset, **but not by the type of '
        'data**. So one thing to do would be to see if we can construct a '
            'dataset such that a model trained on that dataset is _already_ '
            'robust, without adversarial training. The authors do this in the '
            'first experiment. They take an adversarially trained robust '
            'classifier, and create images whose features (final-layer '
            'activations of the robust classifier) match the features of some '
            'unmodified input. The generated images only have robust features '
            'because the original classifier was robust, and in fact models '
            'trained on this dataset are automatically robust.\n'
            '\n'
            'If the "non-robust features" explanation were true, then it should '
            'be possible for a model to learn on a dataset containing only '
            'non-robust features (which will look nonsensical to humans) and '
            '**still generalize to a normal-looking test set**. In the second '
            'experiment (henceforth WrongLabels), the authors construct such a '
            'dataset. Their hypothesis is that adversarial perturbations work by '
            'introducing non-robust features of the target class. So, to '
            'construct their dataset, they take an image x with original label y, '
            "adversarially perturb it towards some class y' to get image x', and "
            "then add (x', y') to their dataset (even though to a human x' looks "
            'like class y). They have two versions of this: in RandLabels, the '
            "target class y' is chosen randomly, whereas in DetLabels, y' is "
            'chosen to be y + 1. For both datasets, if you train a new model on '
            'the dataset, you get good performance **on the original test set**, '
            'showing that the "non-robust features" do generalize.'
        ),
        'title': 'Adversarial Examples Are Not Bugs, They Are Features',
        'url': 'http://gradientscience.org/adv/',
        'venue': 'arXiv',
}
