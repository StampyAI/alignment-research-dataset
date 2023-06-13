from dataclasses import dataclass
from bs4 import BeautifulSoup
from markdownify import MarkdownConverter
import os
import re
import logging
from tqdm import tqdm
from align_data.common.alignment_dataset import AlignmentDataset , DataEntry

logger = logging.getLogger(__name__)

@dataclass
class Distill(AlignmentDataset):

    done_key = 'filename'

    def setup(self):
        super().setup()
        self.files_path = self.raw_data_path / "distill_posts"

    @property
    def items_list(self):
        return self.files_path.glob('*')

    def process_entry(self, filename):
        html = filename.read_text()
        soup = BeautifulSoup(html, "html.parser")
        title = soup.find("title").text
        # find anything with the property 'article:author'
        authors = soup.find_all("meta", {"property": "article:author"})
        # then for each author, get the content
        authors = [author.get("content") for author in authors]

        # same for dates
        date = soup.find("meta", {"property": "article:published"})
        # if content in date is not None, then get the content
        date_published = date.get("content") if date is not None else None

        # find the href with doi in it
        doi = soup.find_all("a", {"href": True})
        doi = [link.get("href") for link in doi if "doi" in link.get("href")]
        doi = doi[0] if len(doi) > 0 else None

        # the body is in the tag d-article
        body = soup.find("d-article")
        if body is None: body = soup.find("dt-article")


        # the abstract is the first ptag in the body
        try:
            abstract = body.find("p").text.replace("\n", " ")
        except:
            abstract = ""
            pass

        md = MarkdownConverter()
        markdown_text = md.convert_soup(body)
        body = markdown_text

        # pull the ol with class references out of the soup
        references = soup.find("ol", {"class": "references"})
        if references:
            # for each reference li, get the the span with the class title
            references = [
                {"title": reference.find("span", {"class": "title"}).text}
                for reference in references.find_all("li")
            ]
            # walk through each li in the references ol, and if it has an a with href, add it to the dict
            for idx in range(len(references)):
                reference = references[idx]
                if reference.get("a") is not None:
                    reference["link"] = reference.get("a").get("href")
                references[idx] = reference
        else:
            references = None

        body = "".join(word for word in re.split("(\n)", body) if len(word) <= 80)
        body = re.sub(r"(?<!\n)\n(?!\n)|\n{3,}", "\n\n", body)

        return DataEntry({
            "url": "n/a",
            "source": "distill",
            "source_type": "html",
            "converted_with": "python",
            "title": title,
            "authors": authors,
            "date_published": str(date_published),
            "abstract": abstract,
            "journal_ref": "distill-pub",
            "doi": doi,
            "text": body,
            "bibliography_bib": references,
            'filename': filename.name
        })
